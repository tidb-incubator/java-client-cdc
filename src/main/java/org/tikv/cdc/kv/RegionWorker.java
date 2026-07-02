package org.tikv.cdc.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCConfig;
import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.model.*;
import org.tikv.common.TiSession;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.util.TimeUtils;
import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.common.util.concurrent.RateLimiter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.tikv.cdc.model.RegionKeyRange.keyInRange;
import static org.tikv.cdc.model.RegionKeyRange.toComparableKey;
import static org.tikv.kvproto.Cdcpb.Event.LogType.INITIALIZED;

/**
 * A Region worker is responsible for all regions in a TiKV store. The Region worker reads the grpc
 * response from its input chan, processes it, and writes it to Puller's eventChan.
 */
public class RegionWorker {
    private static final Logger LOG = LoggerFactory.getLogger(RegionWorker.class);
    private final TiSession tiSession;
    private final EventFeedStream stream;
    private final Consumer<RegionFeedEvent> eventConsumer;
    private final Consumer<RegionErrorInfo> regionErrorConsumer;
    private final RegionTsManager rstManager;
    private final RegionStateManager stateManager;
    private final int workerConcurrency;
    private final AtomicLong eventEntryNumbers = new AtomicLong(0);
    private final AtomicLong commitEventNumbers = new AtomicLong(0);
    private final ScheduledExecutorService resolvedScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread thread = new Thread(r);
                        thread.setName("region-worker-resolved-lock-thread");
                        return thread;
                    });
    RateLimiter resolvedLimiter = RateLimiter.create(0.5);

    public RegionWorker(
            TiSession tiSession,
            EventFeedStream stream,
            Consumer<RegionFeedEvent> eventConsumer,
            Consumer<RegionErrorInfo> regionErrorConsumer,
            CDCConfig cdcConfig) {
        this.tiSession = tiSession;
        this.stream = stream;
        this.eventConsumer = eventConsumer;
        this.regionErrorConsumer = regionErrorConsumer;
        //        this.cdcConfig = cdcConfig;
        this.workerConcurrency = cdcConfig.getWorkerPoolSize();
        //        this.executorService =
        // Executors.newFixedThreadPool(cdcConfig.getWorkerPoolSize());
        this.rstManager = new RegionTsManager();
        resolvedScheduler.scheduleAtFixedRate(this::checkResolveLock, 0, 5, TimeUnit.SECONDS);
        this.stateManager = new RegionStateManager(-1);
        //        stream.getRegions().iter((regionId, state) -> {
        //            LOG.debug("Init region {} state", regionId);
        //            this.stateManager.setState(regionId, state);
        //            return true;
        //        });
    }

    public int inputCalcSlot(long regionId) {
        return (int) (regionId % workerConcurrency);
    }

    public EventFeedStream getStream() {
        return stream;
    }

    public void processEvent(RegionStatefulEvent event) {
        boolean skipEvent =
                event.getRegionFeedState() != null && event.getRegionFeedState().isStale();
        if (skipEvent) {
            LOG.debug("Skip event because state stale. {}", event);
            return;
        }
        //        if (event.getEvent() == null) {
        //            LOG.error("Region processEvent event's Cdcpb.Event is null:{}", event);
        //        }
        if (event.getEvent() != null) {
            if (event.getEvent().hasEntries()) {
                LOG.info(
                        "Metric: Receive event requestId:{}，regionId :{}，event size:{},crts: {},total event entry numbers:{}",
                        event.getEvent().getRequestId(),
                        event.getEvent().getRegionId(),
                        event.getEvent().getEntries().getEntriesList().size(),
                        event.getEvent().getEntries().getEntriesList().get(0).getCommitTs(),
                        eventEntryNumbers.getAndIncrement());
                LOG.info("Metric,debug commit tsl:{}", event.getEvent());
                //                if
                // (event.getEvent().getEntries().getEntriesList().get(0).getCommitTs() == 0) {
                //                    LOG.error("Metric,debug commit ts is null:{}",
                // event.getEvent());
                //                }
                handleEventEntry(event.getEvent().getEntries(), event.getRegionFeedState());
            }
            if (event.getEvent().hasAdmin()) {
                LOG.info("Receive admin event.requestId:{}", event.getEvent().getRequestId());
            }
            if (event.getEvent().hasError()) {
                try {
                    handleSingleRegionError(
                            event.getEvent().getError(), event.getRegionFeedState());
                } catch (Exception e) {
                    LOG.error("Region worker handle region error.", e);
                }
            }
        }
        if (event.getResolvedTsEvent() != null && event.getResolvedTsEvent().getResolvedTs() > 0) {
            //            LOG.info("Region processEvent event.getResolvedTsEvent() != null.
            // ResolvedTsEvent:{}", event.getResolvedTsEvent());
            handleResolvedTs(event.getResolvedTsEvent());
        }
    }

    public void handleEventEntry(
            Cdcpb.Event.Entries entries, RegionStateManager.RegionFeedState state) {
        long regionId = state.getRegionId();
        RegionKeyRange.ComparableKeyRange comparableKeyRange = state.getKeyRange();
        for (Cdcpb.Event.Row event : entries.getEntriesList()) {
            //            LOG.info("Region processEvent event.getEvent() != null.
            // handleEventEntry:{}", event.toString());
            MutableEventRow mutableEventRow = new MutableEventRow(event);
            byte[] comparableKey = toComparableKey(event.getKey());
            if (event.getType() != INITIALIZED && !keyInRange(comparableKey, comparableKeyRange)) {
                LOG.info(
                        "Dropping event because event is out of range. Key {}, is not in [KeyRange {}]",
                        event.getKey(),
                        KeyUtils.formatBytesUTF8(RegionKeyRange.toKeyRange(comparableKeyRange)));
                continue;
            }
            switch (event.getType()) {
                case INITIALIZED:
                    state.setInitialized();
                    for (MutableEventRow row : state.getMatcher().matchCachedRow(true)) {
                        RegionFeedEvent regionFeedEvent =
                                RegionFeedEvent.assembleRowEvent(regionId, row.getEventRow());
                        eventConsumer.accept(regionFeedEvent);
                    }
                    state.getMatcher().matchCachedRollbackRow(true);
                    break;
                case COMMITTED:
                    long resolveTs = state.getLastResolvedTs();
                    if (event.getCommitTs() <= resolveTs) {
                        LOG.error(
                                "The CommitTs must be greater than the resolvedTs.EventTyp:{},CommitTs:{},resolvedTs:{},regionId:{}",
                                "COMMITTED",
                                event.getCommitTs(),
                                resolveTs,
                                regionId);
                        // todo
                    }
                    RegionFeedEvent regionFeedEvent =
                            RegionFeedEvent.assembleRowEvent(
                                    regionId, mutableEventRow.getEventRow());
                    eventConsumer.accept(regionFeedEvent);
                    break;
                case PREWRITE:
                    MutableEventRow mutableCommitEventRow =
                            state.getMatcher()
                                    .matchCommitRow(mutableEventRow, state.isInitialized());
                    if (mutableCommitEventRow != null) {
                        LOG.error(
                                "Metric: Region {} has match precommit event {} and postwrite event {} , has received commit event number {}",
                                regionId,
                                mutableCommitEventRow.getEventRow(),
                                mutableEventRow.getEventRow(),
                                commitEventNumbers.getAndIncrement());
                        // 正常发射prewrite事件
                        eventConsumer.accept(
                                RegionFeedEvent.assembleRowEvent(
                                        regionId, mutableEventRow.getEventRow()));
                        // 重新发射commit事件
                        eventConsumer.accept(
                                RegionFeedEvent.assembleRowEvent(
                                        regionId, mutableCommitEventRow.getEventRow()));
                        break;
                    }
                    state.getMatcher().putPrewriteRow(mutableEventRow);
                    break;
                case COMMIT:
                    if (!state.getMatcher().matchRow(mutableEventRow, state.isInitialized())) {
                        if (!state.isInitialized()) {
                            state.getMatcher().cacheCommitRow(mutableEventRow);
                            LOG.error(
                                    "The Event:{},The EventTyp:{},!state.isInitialized():{}",
                                    mutableEventRow.toString(),
                                    "COMMIT",
                                    !state.isInitialized());
                            // 没匹配上，下次prewrite来的时候再匹配
                            state.getMatcher().putCommitRow(mutableEventRow);
                            break;
                        }
                        // todo ErrPrewriteNotMatch
                        LOG.error(
                                "The Event:{},The EventTyp:{},state.isInitialized():{}",
                                mutableEventRow.toString(),
                                "COMMIT",
                                state.isInitialized());
                        // 没匹配上，下次prewrite来的时候再匹配
                        state.getMatcher().putCommitRow(mutableEventRow);
                        break;
                    }
                    // boolean isStaleEvent = event.getCommitTs() < startTs;
                    long resolvedTs = state.getLastResolvedTs();
                    if (event.getCommitTs() < resolvedTs) {
                        LOG.error(
                                "The CommitTs must be greater than the resolvedTs.EventTyp:{},CommitTs:{},resolvedTs:{},regionId:{}",
                                "COMMIT",
                                event.getCommitTs(),
                                resolvedTs,
                                regionId);
                        // return;
                        // todo errUnreachable
                    }
                    LOG.debug(
                            "Metric: Region {} has received commit event number {}",
                            regionId,
                            commitEventNumbers.getAndIncrement());
                    eventConsumer.accept(
                            RegionFeedEvent.assembleRowEvent(
                                    regionId, mutableEventRow.getEventRow()));
                    break;
                case ROLLBACK:
                    if (!state.isInitialized()) {
                        state.getMatcher().cacheRollbackRow(mutableEventRow);
                        continue;
                    }
                    state.getMatcher().rollbackRow(mutableEventRow);
                    break;
                default:
                    LOG.warn(
                            "Un handler event entry.eventType:{},eventKey:{}, regionId:{}",
                            event.getType(),
                            event.getKey(),
                            regionId);
                    break;
            }
        }
        LOG.info("RegionWorker handleEventEntry eventConsumer:{}", eventConsumer.toString());
    }

    public void handleSingleRegionError(Cdcpb.Error error, RegionStateManager.RegionFeedState state)
            throws Exception {
        long regionId = state.getRegionId();
        boolean isStale = state.isStale();
        LOG.info(
                "Single Region event feed disconnected, regionId [{}],resolved ts [{}], isStale [{}]",
                state.getRegionId(),
                state.getLastResolvedTs(),
                state.isStale());
        if (isStale) {
            checkShouldExit();
        }
        state.markStopped();
        delRegionState(regionId);

        if (error.hasDuplicateRequest()) {
            LOG.info("meet Duplicate request error, cancel the gPRC steam");
            this.stream.cancelStream(1);
        }
        RegionErrorInfo errorInfo = new RegionErrorInfo(state.getSri(), error);
        try {
            this.tiSession
                    .getRegionManager()
                    .onRequestFail(
                            this.tiSession
                                    .getRegionManager()
                                    .getRegionById(
                                            errorInfo.getSingleRegionInfo().getVerID().getId()));
        } catch (GrpcException e) {
            LOG.error(
                    "Session getRegionId fail during handle region notleader error:{}",
                    e.getMessage(),
                    e);
        } finally {
            this.regionErrorConsumer.accept(errorInfo);
        }
    }

    private boolean checkRegionStateEmpty() {
        for (RegionStateManager.SyncRegionFeedStateMap stateMap : this.stateManager.getStates()) {
            if (stateMap.size() != 0) {
                return false;
            }
        }
        return true;
    }

    public void checkShouldExit() throws Exception {
        boolean empty = checkRegionStateEmpty();

        if (empty && this.stream.getRegions().size() == 0) {
            LOG.info(
                    "A single region error happens before. and there is no region maintained by this region worker, exit it and cancel the gRPC stream.");
            this.stream.cancelStream(1);
            throw new ClientException("Error region worker exit.");
        }
    }

    public void evictAllRegions() {
        Map<Long, RegionStateManager.RegionFeedState> deletes = new ConcurrentHashMap<>();
        for (RegionStateManager.SyncRegionFeedStateMap states : this.stateManager.getStates()) {
            states.iter(
                    (regionId, state) -> {
                        if (state.isStale()) {
                            return true;
                        }
                        state.markStopped();
                        deletes.put(regionId, state);
                        return true;
                    });
            try {
                deletes.forEach(
                        (regionId, state) -> {
                            delRegionState(regionId);
                            // throw region error;
                            this.tiSession
                                    .getRegionManager()
                                    .onRequestFail(
                                            this.tiSession
                                                    .getRegionManager()
                                                    .getRegionById(regionId));
                        });
            } catch (Exception e) {
                LOG.info("RegionWorker evictAllRegions getRegionById exception", e);
            }
        }
    }

    public void handleResolvedTs(RegionStatefulEvent.ResolvedTsEvent resolvedTsEvent) {
        long resolvedTs = resolvedTsEvent.getResolvedTs();
        List<RegionKeyRange> regionKeyRange = new ArrayList<>();
        List<Long> regions = new ArrayList<>();
        for (RegionStateManager.RegionFeedState state : resolvedTsEvent.getRegions()) {
            if (state.isStale() || !state.isInitialized()) {
                continue;
            }
            long regionID = state.getRegionId();
            regions.add(regionID);
            long lastResolvedTs = state.getLastResolvedTs();
            if (resolvedTs < lastResolvedTs) {
                LOG.info(
                        "The resolvedTs is fallen back in kvclient.EventType:{},resolvedTs:{},lastResolvedTs{},regionId:{}",
                        "RESOLVED",
                        resolvedTs,
                        lastResolvedTs,
                        regionID);
                continue;
            }
            regionKeyRange.add(new RegionKeyRange(regionID, state.getSri().getSpan()));
        }
        if (regionKeyRange.isEmpty()) {
            return;
        }
        resolveLock(new RtsUpdateEvent(regions, resolvedTs));
        for (RegionStateManager.RegionFeedState state : resolvedTsEvent.getRegions()) {
            if (state.isStale() || !state.isInitialized()) {
                continue;
            }
            state.updateResolvedTs(resolvedTs);
        }
        RegionFeedEvent rEvent = new RegionFeedEvent();
        RegionFeedEvent.ResolvedKeyRanges resolvedKeyRanges =
                new RegionFeedEvent.ResolvedKeyRanges();
        resolvedKeyRanges.setResolvedTs(resolvedTs);
        resolvedKeyRanges.setKeyRanges(regionKeyRange);
        rEvent.setResolved(resolvedKeyRanges);
        rEvent.setRawKVEntry(null);
        eventConsumer.accept(rEvent);
    }

    public void resolveLock(RtsUpdateEvent rtsUpdateEvent) {
        Instant eventTime = Instant.now();
        for (long regionId : rtsUpdateEvent.getRegions()) {
            rstManager.upsert(regionId, rtsUpdateEvent.getResolvedTs(), eventTime);
        }
    }

    public void checkResolveLock() {
        TiTimestamp currentTimeFromPD = tiSession.getTimestamp();
        int resolveLockPenalty = 10;
        Duration resolveLockInterval = Duration.ofSeconds(20);
        List<RegionTsManager.RegionTsInfo> expired = new ArrayList<>();
        while (this.rstManager.size() > 0) {
            RegionTsManager.RegionTsInfo item = rstManager.pop();
            Duration sinceLastResolvedTs =
                    TimeUtils.sub(
                            Instant.ofEpochMilli(item.getTs().getResolvedTs()),
                            Instant.ofEpochMilli(currentTimeFromPD.getPhysical()));
            if (sinceLastResolvedTs.compareTo(resolveLockInterval) < 0) {
                this.rstManager.upsert(
                        item.getRegionId(),
                        item.getTs().getResolvedTs(),
                        item.getTs().getEventTime());
                break;
            }
            expired.add(item);
            if (expired.size() > 64) {
                break;
            }
            if (expired.size() == 0) {
                continue;
            }
            for (RegionTsManager.RegionTsInfo rts : expired) {
                RegionStateManager.RegionFeedState state =
                        this.stateManager.getState(rts.getRegionId());
                if (state == null || state.isStale()) {
                    continue;
                }
                long lastResolvedTs = state.getLastResolvedTs();
                sinceLastResolvedTs =
                        TimeUtils.sub(
                                Instant.ofEpochMilli(lastResolvedTs),
                                Instant.ofEpochMilli(currentTimeFromPD.getPhysical()));
                if (sinceLastResolvedTs.compareTo(resolveLockInterval) >= 0) {
                    Duration sinceLastEvent = TimeUtils.timeSince(rts.getTs().getEventTime());
                    if (sinceLastResolvedTs.compareTo(Duration.ofMinutes(40)) > 0
                            && sinceLastEvent.compareTo(Duration.ofMinutes(40)) > 0) {
                        LOG.warn(
                                "kv client reconnect triggered,duration {}, sinceLastEvent{}",
                                sinceLastResolvedTs,
                                sinceLastEvent);
                        throw new ClientException("Internal error, reconnect all regions.");
                    }
                    if (rts.getTs().getPenalty() < resolveLockPenalty) {
                        if (lastResolvedTs > rts.getTs().getResolvedTs()) {
                            rts.getTs().setResolvedTs(lastResolvedTs);
                            rts.getTs().setEventTime(Instant.now());
                            rts.getTs().setPenalty(0);
                        }
                        this.rstManager.insert(rts);
                        continue;
                    }
                    if (resolvedLimiter.tryAcquire(1000)) {
                        LOG.warn(
                                "region not receiving resolved event from tikv or resolved ts is not pushing for too long times.");
                    }
                    // todo session resolve lock
                    rts.getTs().setPenalty(0);
                }
                rts.getTs().setResolvedTs(lastResolvedTs);
                this.rstManager.insert(rts);
            }
        }
    }

    public void processEvents(List<RegionStatefulEvent> rsEvents) {
        for (RegionStatefulEvent rse : rsEvents) {
            processEvent(rse);
        }
    }

    public int getWorkerConcurrency() {
        return workerConcurrency;
    }

    public void setRegionState(long regionId, RegionStateManager.RegionFeedState state) {
        this.stateManager.setState(regionId, state);
    }

    public RegionStateManager.RegionFeedState getRegionState(long regionId) {
        return this.stateManager.getState(regionId);
    }

    public RegionStateManager getStateManager() {
        return stateManager;
    }

    public void delRegionState(long regionId) {
        this.stateManager.delState(regionId);
    }

    public static class RtsUpdateEvent {
        List<Long> regions;
        long resolvedTs;

        public RtsUpdateEvent(List<Long> regions, long resolvedTs) {
            this.regions = regions;
            this.resolvedTs = resolvedTs;
        }

        public List<Long> getRegions() {
            return regions;
        }

        public void setRegions(List<Long> regions) {
            this.regions = regions;
        }

        public long getResolvedTs() {
            return resolvedTs;
        }

        public void setResolvedTs(long resolvedTs) {
            this.resolvedTs = resolvedTs;
        }
    }
}

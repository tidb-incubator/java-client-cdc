package org.tikv.cdc.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCConfig;
import org.tikv.cdc.TableStoreStat;
import org.tikv.cdc.TableStoreStats;
import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.frontier.Frontier;
import org.tikv.cdc.frontier.KeyRangeFrontier;
import org.tikv.cdc.model.OperateType;
import org.tikv.cdc.model.PolymorphicEvent;
import org.tikv.cdc.model.RawKVEntry;
import org.tikv.cdc.model.RegionErrorInfo;
import org.tikv.cdc.model.RegionFeedEvent;
import org.tikv.cdc.model.RegionKeyRange;
import org.tikv.cdc.model.RegionStatefulEvent;
import org.tikv.cdc.model.RegionVerId;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.GrpcException;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.IDAllocator;
import org.tikv.common.util.RangeSplitter;
import org.tikv.common.util.TableKeyRangeUtils;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Metapb;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static org.tikv.cdc.model.RegionKeyRange.intersect;
import static org.tikv.cdc.model.RegionKeyRange.toComparableKeyRange;
import static org.tikv.cdc.model.RegionKeyRange.toKeyRange;

public class CDCClient {
    private static final Logger LOG = LoggerFactory.getLogger(CDCClient.class);
    public static final long NO_STOPPING_TS = Long.MAX_VALUE;
    private final CDCConfig cdcConfig;
    private final TiSession tiSession;
    private final BlockingQueue<RegionFeedEvent> eventsBuffer;
    private final ConcurrentHashMap<String, EventFeedStream> storeStreamCache =
            new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<RegionStatefulEvent> resolveTsPool =
            new ConcurrentLinkedQueue<>();
    private final TableStoreStats tableStoreStats = new TableStoreStats();
    private final AtomicLong startResolvedTs = new AtomicLong(-1);
    private final AtomicLong checkpointTs = new AtomicLong(-1);
    private final AtomicLong resolvedTs = new AtomicLong(-1);
    private Optional<TiTableInfo> tableInfoOptional = Optional.empty();
    private Instant lastForwardTime = Instant.now();
    private final AtomicLong lastForwardResolvedTs = new AtomicLong(-1);

    private final Consumer<RegionFeedEvent> eventConsumer;
    private final Consumer<RegionErrorInfo> errorInfoConsumer;
    private final List<EventListener> listeners = new ArrayList<>();
    public static final int NO_LEADER_STORE_ID = 0;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong rawKVEntryNumber = new AtomicLong(0);

    private final Object tableStatsLock = new Object();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    // todo ,to config. xx/s
    private final long resolveTsStuckInterval = Duration.ofSeconds(60).getSeconds();
    private final ExecutorService clientExecutor =
            Executors.newFixedThreadPool(
                    1,
                    new ThreadFactory() {
                        private int threadCount = 1;

                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "CDC-Client-" + threadCount++);
                        }
                    });
    private final List<CompletableFuture> cfs = new ArrayList<>(1);
    private final String dbName;
    private final String tableName;
    private final long endTs;
    private Frontier tsTracker;
    private int eventCount = 0;
    private final long rate;
    private ScheduledFuture<?> stuckDetectorTicker;
    public CDCClient(
            TiConfiguration tiConf,
            final String dbName,
            final String tableName,
            final long endTs,
            CDCConfig cdcConfig) {
        this(tiConf, cdcConfig, dbName, tableName, endTs);
    }

    public CDCClient(
            TiConfiguration tiConf,
            CDCConfig cdcConfig,
            final String dbName,
            final String tableName,
            final long endTs) {
        this.cdcConfig = cdcConfig;
        this.tiSession = new TiSession(tiConf);
        this.dbName = dbName;
        this.tableName = tableName;
        this.endTs = endTs;
        this.rate = 1000L;
        //        eventsBuffer = new LinkedBlockingQueue<>(cdcConfig.getEventBufferSize());
        LOG.info("eventbuffer all size :{}", cdcConfig.getEventBufferSize());
        eventsBuffer =
                new PriorityBlockingQueue<>(
                        cdcConfig.getEventBufferSize(),
                        Comparator.comparingLong(a -> a.getResolved().getResolvedTs()));
        // fix: use queue.put() instead of queue.offer(), otherwise will lose event
        eventConsumer =
                (event) -> {
                    // try 2 times offer.
                    for (int i = 0; i < 2; i++) {
                        if (eventsBuffer.offer(event)) {
                            eventCount++;
                            if (eventCount % 1000 == 0) {
                                LOG.info("eventbuffer size :{}", eventsBuffer.size());
                            }
                            return;
                        }
                    }
                    // else use put.
                    try {
                        eventsBuffer.put(event);
                    } catch (InterruptedException e) {
                        LOG.error("Events buffer put error!", e);
                    }
                };
        this.errorInfoConsumer =
                (ed) -> {
                    try {
                        handleError(ed);
                    } catch (ClientException e) {
                        LOG.error("Handle error!", e);
                        for (EventListener listener : listeners) {
                            listener.onException(e);
                        }
                    }
                };
    }

    public void start(long startTs) {
        LOG.info(
                "Start cdc client at time {},database {} table {} listening.",
                startTs,
                dbName,
                tableName);
        startTs = startTs - 20000000000L; // 30s
        this.startResolvedTs.set(startTs);
        LOG.info(
                "Start cdc client at new time {},database {} table {} listening.",
                startTs,
                dbName,
                tableName);
        this.checkpointTs.set(startTs);
        this.stuckDetectorTicker =
                scheduler.scheduleAtFixedRate(
                        () -> {
                            try {
                                detectResolvedTsStuck();
                            } catch (ClientException e) {
                                LOG.error("Detect resolvedTs stuck error!", e);
                                for (EventListener listener : listeners) {
                                    listener.onException(e);
                                }
                                if (stuckDetectorTicker !=null){
                                    stuckDetectorTicker.cancel(false);
                                }
                                try {
                                    close();
                                }catch (Exception exception){
                                    LOG.error("failed to close cdcclient ",exception);
                                }
//                                close();
                            }
                        },
                        5,
                        30,
                        TimeUnit.SECONDS);
        if (started.compareAndSet(false, true)) {
            CompletableFuture<Void> cf =
                    CompletableFuture.runAsync(
                            () -> { // try insert retry Mechanism
                                Optional<TiTableInfo> tableInfoOptional =
                                        getTableInfo(dbName, tableName);
                                this.tableInfoOptional = tableInfoOptional;
                                if (!tableInfoOptional.isPresent()) {
                                    LOG.error("Get tableInfo for {}.{} failed!", dbName, tableName);
                                    throw new ClientException(
                                            String.format(
                                                    "Get tableInfo for %s.%s failed.",
                                                    dbName, tableName));
                                }
                                KeyRange keyRange =
                                        TableKeyRangeUtils.getTableKeyRange(
                                                tableInfoOptional.get().getId());

                                // new ts tracker;
                                RegionKeyRange.ComparableKeyRange comparableKeyRange =
                                        toComparableKeyRange(keyRange);
                                tsTracker = new KeyRangeFrontier(0, comparableKeyRange);
                                List<RegionStateManager.SingleRegionInfo> singleRegionInfos =
                                        divideToRegions(comparableKeyRange, false);
                                for (RegionStateManager.SingleRegionInfo sri : singleRegionInfos) {
                                    try {
                                        requestRegionToStore(
                                                sri, tableInfoOptional.get().getId(), -1);
                                    } catch (ClientException | CancellationException ce) {
                                        for (EventListener eventListener : listeners) {
                                            if (ce instanceof CancellationException) {
                                                eventListener.onException(new ClientException(ce));
                                            } else {
                                                eventListener.onException((ClientException) ce);
                                            }
                                        }
                                    }
                                }
                                boolean initialized = false;
                                long lastResolvedTs = this.checkpointTs.get();
                                Instant lastAdvancedTime = Instant.now();
                                Instant lastLogSlowRangeTime = Instant.now();
                                while (isRunning()
                                        && (endTs == NO_STOPPING_TS || lastResolvedTs <= endTs)) {
                                    try {
                                        for (EventListener eventListener : listeners) {
                                            RegionFeedEvent regionFeedEvent = null;
                                            try {
                                                regionFeedEvent = eventsBuffer.take();
                                            } catch (InterruptedException e) {
                                                continue;
                                            }
                                            if (regionFeedEvent == null) {
                                                LOG.info("regionFeedEvent null");
                                                continue;
                                            }
                                            // output.先消费事件，再打印时间戳
                                            if (regionFeedEvent.getRawKVEntry() != null) {
                                                if (LOG.isDebugEnabled()) {
//                                                    LOG.debug(
//                                                            "Metric: received raw kv entry numbers is {}",
//                                                            rawKVEntryNumber.incrementAndGet());
                                                }
                                                eventListener.notify(
                                                        output(
                                                                regionFeedEvent.getRawKVEntry(),
                                                                tableInfoOptional));
                                            }
                                            if (regionFeedEvent.getResolved() != null
                                                    && regionFeedEvent.getResolved().getKeyRanges()
                                                    != null) {
                                                for (RegionKeyRange kr :
                                                        regionFeedEvent
                                                                .getResolved()
                                                                .getKeyRanges()) {
                                                    if (!kr.isSubKeyRange(
                                                            kr.getKeyRange(), comparableKeyRange)) {
                                                        LOG.warn(
                                                                "Resolved ts,2:region-{},ts-{},keyRange-{},tableKeyRange-{}",
                                                                kr.getRegionId(),
                                                                regionFeedEvent
                                                                        .getResolved()
                                                                        .getResolvedTs(),
                                                                kr.getKeyRange(),
                                                                comparableKeyRange);
                                                    }
                                                    // todo comparable
//                                                    if (!initialized){
                                                        tsTracker.forward(
                                                                kr.getRegionId(),
                                                                kr.getKeyRange(),
                                                                regionFeedEvent
                                                                        .getResolved()
                                                                        .getResolvedTs());
//                                                    }
                                                }
                                                long resolvedTs = tsTracker.frontier();
                                                LOG.info(
                                                        "resolvedTs = {},lastResolvedTs = {},endTs = {}",
                                                        resolvedTs,
                                                        lastResolvedTs,
                                                        endTs);
                                                if (resolvedTs > 0 && !initialized) {
                                                    initialized = true;
                                                    LOG.info(
                                                            "Puller is initialized.Resolved Ts is {}",
                                                            resolvedTs);
                                                }
                                                if (!initialized) {
                                                    LOG.warn(
                                                            "Initialized Status is {}",
                                                            initialized);
                                                    continue;
                                                }
                                                if (resolvedTs <= lastResolvedTs) {
                                                    if (Duration.between(
                                                                    lastAdvancedTime,
                                                                    Instant.now())
                                                            .getSeconds()
                                                            > 30
                                                            && Duration.between(
                                                                    lastLogSlowRangeTime,
                                                                    Instant.now())
                                                            .getSeconds()
                                                            > 30 /*30s*/) {
                                                        final long[] slowestTs = {Long.MAX_VALUE};
                                                        AtomicBoolean rangeFilled =
                                                                new AtomicBoolean(true);
                                                        RegionKeyRange.ComparableKeyRange
                                                                slowestRange =
                                                                new RegionKeyRange
                                                                        .ComparableKeyRange(
                                                                        null, null);
                                                        tsTracker.entries(
                                                                (key, ts) -> {
                                                                    if (ts < slowestTs[0]) {
                                                                        slowestTs[0] = ts;
                                                                        slowestRange.setStart(key);
                                                                        rangeFilled.set(false);
                                                                    } else if (!rangeFilled.get()) {
                                                                        slowestRange.setEnd(key);
                                                                        rangeFilled.set(true);
                                                                    }
                                                                });
                                                        LOG.debug(
                                                                "Table puller has been stucked, tableName:{},resolvedTs:{},slowestRangeTs:{},range:{}",
                                                                tableName,
                                                                resolvedTs,
                                                                slowestTs,
                                                                slowestRange);
                                                        lastLogSlowRangeTime = Instant.now();
                                                    }
                                                    continue;
                                                }
                                                LOG.info(
                                                        "Current {}.{}.Resolved ts 3: {}",
                                                        dbName,
                                                        tableName,
                                                        lastResolvedTs);
                                                lastResolvedTs = resolvedTs;
                                                lastAdvancedTime = Instant.now();

                                                // 这里是output时间戳事件
                                                output(
                                                        new RawKVEntry.Builder()
                                                                .setCrts(lastResolvedTs)
                                                                .setOpType(OperateType.Resolved)
                                                                .setRegionId(
                                                                        regionFeedEvent
                                                                                .getRegionId())
                                                                .build(),
                                                        tableInfoOptional);
                                                this.resolvedTs.getAndSet(resolvedTs);
                                            }
                                        } //
                                    } catch (Exception e) {
                                        for (EventListener eventListener : listeners) {
                                            eventListener.onException(new ClientException(e));
                                        }
                                    }
                                }
                                LOG.warn("CDC client stopped. running status is {}", isRunning());
                            },
                            clientExecutor);
            cfs.add(cf);
        }
        // add a shutdown hook to trigger the stop the process
        //测试  先不启动stuckdetectorTicker
        stuckDetectorTicker.cancel(true);
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    private PolymorphicEvent output(RawKVEntry raw, Optional<TiTableInfo> tableInfoOptional) {
        if (raw.getCrts() < this.resolvedTs.get()
                || (raw.getCrts() == this.resolvedTs.get()
                && !OperateType.Resolved.equals(raw.getOpType()))) {
            LOG.warn(
                    "The crts is fallen back in puller.Schema is {}.{}.commitTs:{} < ResolveTs:{}",
                    dbName,
                    tableName,
                    raw.getCrts(),
                    this.resolvedTs.get());
            return null;
        }
        if (this.checkpointTs.get() < raw.getCrts()) {
            this.checkpointTs.set(raw.getCrts());
        }

        return new PolymorphicEvent(
                raw.getStartTs(), raw.getCrts(), dbName, raw, tableInfoOptional.get());
    }

    public synchronized Optional<TiTableInfo> getTableInfo(String dbName, String tableName) {
        return Optional.ofNullable(this.tiSession.getCatalog().getTable(dbName, tableName));
    }

    public synchronized void addListener(EventListener listener) {
        this.listeners.add(listener);
    }

    public boolean isRunning() {
        return started.get();
    }

    public void join() {
        if (started.get()) {
            cfs.forEach(
                    cf -> {
                        CompletableFuture.allOf(cf).join();
                    });
        }
    }

    public synchronized void close() {
        try {
            // client stopped.
            LOG.info("Start close cdc client.");
            started.set(false);
            this.stuckDetectorTicker.cancel(false);
            for (EventFeedStream stream : storeStreamCache.values()) {
                LOG.info("Close event feed stream. ip add is {}.", stream.getAddr());
                stream.close();
            }
            LOG.info("Clear event feed stream cache.");
            storeStreamCache.clear();
            LOG.info("Close tiSession.");
            this.tiSession.close();
            // shutdown executor.
            LOG.info("shutdown cdc client.");
            clientExecutor.shutdownNow();
            long clientClosedTimeoutSeconds = 30L;
            if (!clientExecutor.awaitTermination(clientClosedTimeoutSeconds, TimeUnit.SECONDS)) {
                LOG.error(
                        "Failed to close the cdc client in {} seconds.Force shutdown now.",
                        clientClosedTimeoutSeconds);
                clientExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Closed Client exception.", e);
            throw new ClientException(e);
        } catch (IOException e) {
            throw new ClientException(e);
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    private List<RegionStateManager.SingleRegionInfo> divideToRegions(
            RegionKeyRange.ComparableKeyRange keyRange, boolean warmUp) {
        if (warmUp) {
            this.tiSession.warmUp();
        }
        final RangeSplitter splitter = RangeSplitter.newSplitter(tiSession.getRegionManager());
        final List<TiRegion> tiRegionList =
                splitter.splitRangeByRegion(Arrays.asList(toKeyRange(keyRange))).stream()
                        .map(RangeSplitter.RegionTask::getRegion)
                        .sorted((a, b) -> Long.compare(a.getId(), b.getId()))
                        .collect(Collectors.toList());
        List<RegionStateManager.SingleRegionInfo> singleRegionInfos = new ArrayList<>();
        LOG.info(
                "Table {}.{} keyRange is {},regionSize is {}",
                this.dbName,
                this.tableName,
                keyRange,
                tiRegionList.size());
        tiRegionList.forEach(
                tiRegion -> {
                    final String address =
                            this.tiSession
                                    .getRegionManager()
                                    .getStoreById(tiRegion.getLeader().getStoreId())
                                    .getStore()
                                    .getAddress();
                    RPCContext rpcContext =
                            new RPCContext.Builder()
                                    .setRegion(tiRegion.getVerID())
                                    .setAddress(address)
                                    .setMeta(tiRegion.getMeta())
                                    .setChannel(this.tiSession.getChannelFactory())
                                    .setHostMapping(this.tiSession.getPDClient().getHostMapping())
                                    .setPeer(tiRegion.getLeader())
                                    .setTiStore(
                                            this.tiSession
                                                    .getRegionManager()
                                                    .getStoreById(
                                                            tiRegion.getLeader().getStoreId()))
                                    .build();
                    LOG.info(
                            "new divideToRegions address:{} , tisession:{}",
                            address,
                            this.tiSession
                                    .getRegionManager()
                                    .getStoreById(tiRegion.getLeader().getStoreId()));
                    // todo devide keyRange to paritial keyRange;
                    try {
                        RegionKeyRange.ComparableKeyRange paritialKeyRange =
                                intersect(
                                        keyRange,
                                        toComparableKeyRange(
                                                KeyRange.newBuilder()
                                                        .setStart(tiRegion.getStartKey())
                                                        .setEnd(tiRegion.getEndKey())
                                                        .build()));
                        paritialKeyRange.hack();
                        LOG.debug(
                                "Divide table to {}.{} partition region {}, startKey {}, endKey {},key range {}",
                                dbName,
                                tableName,
                                tiRegion.getId(),
                                tiRegion.getStartKey(),
                                tiRegion.getEndKey(),
                                paritialKeyRange);
                        RegionStateManager.SingleRegionInfo signalRegionInfo =
                                new RegionStateManager.SingleRegionInfo(
                                        RegionVerId.fromTiRegion(tiRegion.getVerID()),
                                        paritialKeyRange,
                                        rpcContext);
                        signalRegionInfo.setResolvedTs(this.checkpointTs.get());
                        singleRegionInfos.add(signalRegionInfo);
                    } catch (Exception e) {
                        throw new ClientException(e);
                    }
                });
        return singleRegionInfos;
    }

    private void requestRegionToStore(
            RegionStateManager.SingleRegionInfo sri, long tableId, long ts) {
        LOG.info("request region to sri {}", sri);
        long requestId = IDAllocator.allocateRequestID();
        Cdcpb.Header header =
                Cdcpb.Header.newBuilder()
                        .setTicdcVersion(TiDBVersion.V6_5.getVersion())
                        .setClusterId(this.tiSession.getPDClient().getClusterId())
                        .build();

        if (ts < 0) {
            ts = sri.getResolvedTs();
        }
        final Cdcpb.ChangeDataRequest request =
                Cdcpb.ChangeDataRequest.newBuilder()
                        .setRequestId(requestId)
                        .setHeader(header)
                        .setRegionId(sri.getRpcCtx().getRegion().getId())
                        .setCheckpointTs(ts)
                        .setStartKey(sri.getRpcCtx().getMeta().getStartKey())
                        .setEndKey(sri.getRpcCtx().getMeta().getEndKey())
                        .setRegionEpoch(sri.getRpcCtx().getMeta().getRegionEpoch())
                        .setExtraOp(Kvrpcpb.ExtraOp.ReadOldValue)
                        .setFilterLoop(false)
                        .build();
        String storeAddr = sri.getRpcCtx().getAddress();
        long storeId = sri.getRpcCtx().getTiStore().getId();
        EventFeedStream streamClient = storeStreamCache.get(storeAddr);
        if (!storeStreamCache.containsKey(storeAddr) || streamClient.getIsCanceled()) {
            if (storeStreamCache.containsKey(storeAddr)) {
                deleteStream(streamClient);
            }
            EventFeedStream stream =
                    new EventFeedStream(
                            storeAddr,
                            storeId,
                            sri.getRpcCtx(),
                            (long) this.cdcConfig.getEventRateLimit());
            storeStreamCache.put(storeAddr, stream);
            LOG.info(
                    "creating new stream {} to store {} to send request，now total stream number {}",
                    stream.getStreamId(),
                    storeAddr,
                    storeStreamCache.size());
            streamClient = stream;
        }
        RegionStateManager.RegionFeedState state =
                new RegionStateManager.RegionFeedState(sri, requestId);
        streamClient.getRegions().setByRequestID(requestId, state);
        try {
            LOG.info("start step1");
            receiveFromStream(streamClient, request, tableId);
            LOG.info(
                    "start new request.tableID {},streamId {},regionID {}, storeAdd {}",
                    tableId,
                    streamClient.getStreamId(),
                    sri.getRpcCtx().getRegion().getId(),
                    storeAddr);
        } catch (Throwable t) {
            try {
                LOG.error("Request to store failed", t);
                streamClient.close();
            } catch (Exception ex) {
                throw new ClientException("Closed stream client failed", ex);
            }
            // todo sendRequestToStoreError.
            // Delete the stream from the cache so that when next time a region of
            // this store is requested, a new stream to this store will be created.
            deleteStream(streamClient);
            // Remove the region from pendingRegions. If it's already removed, it should be already
            // retried by `receiveFromStream`, so no need to retry here.
            streamClient.getRegions().takeByRequestID(requestId);
            throw new ClientException("requestRegionToStore ClientException");
        }
    }

    private void receiveFromStream(
            EventFeedStream stream, Cdcpb.ChangeDataRequest request, long tableId) {
        LOG.info("receiveFromStream start");

        // 只在操作tableStoreStats时加锁
        synchronized (tableStatsLock) {
            String key = String.format("%d_%s", tableId, stream.getStoreId());
            if (!tableStoreStats.containsKey(key)) {
                tableStoreStats.put(key, new TableStoreStat());
            }
        }
        // set worker.
        RegionWorker worker = stream.getRegionWorker();
        LOG.info("receiveFromStream start step1");
        if (worker != null) {
            RegionStateManager.RegionFeedState regionFeedState =
                    stream.getRegions().takeByRequestID(request.getRequestId());
            regionFeedState.start();
            worker.setRegionState(request.getRegionId(), regionFeedState);
        } else {
            worker =
                    new RegionWorker(
                            tiSession, stream, eventConsumer, errorInfoConsumer, cdcConfig);
            stream.setRegionWorker(worker);
            LOG.warn(
                    "Stream {} new a worker,state manager {}",
                    stream.getStreamId(),
                    worker.getStateManager());
        }
        LOG.info("receiveFromStream start step2");
        try {
            boolean started = stream.StartReceiver(request);
            LOG.info("Grpc Receiver started status: {}", started);
        } catch (Throwable t) {
            LOG.error("receiveFromStream throwable", t);
            throw new ClientException("Grpc StartReceiver Exception",t);
        }

    }

    private void deleteStream(EventFeedStream deleteStreamClient) {
        EventFeedStream regionStreamClientInMap =
                storeStreamCache.get(deleteStreamClient.getAddr());
        if (regionStreamClientInMap == null) {
            LOG.warn(
                    "Delete stream {} failed, stream not found,ignore it",
                    deleteStreamClient.getAddr());
            return;
        }
        if (regionStreamClientInMap.getStreamId() != deleteStreamClient.getStreamId()) {
            LOG.warn(
                    "Delete stream {} failed, stream id mismatch,ignore it",
                    deleteStreamClient.getAddr());
            return;
        }
        if (Duration.between(deleteStreamClient.getCreateTime(), Instant.now()).getSeconds() < 1) {
            LOG.warn(
                    "It's too soon to delete a stream {}, wait for a while,sinceCreateDuration {}",
                    deleteStreamClient.getStreamId(),
                    deleteStreamClient.getCreateTime());
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException", e);
            }
        }
        try {
            deleteStreamClient.close();
        } catch (Exception e) {
            LOG.error("Region stream client {} closed failed.", deleteStreamClient.getAddr(), e);
            throw new ClientException(e);
        }
        storeStreamCache.remove(deleteStreamClient.getAddr());
        LOG.info(
                "Region stream client id {}, storeId {} has been removed.",
                deleteStreamClient.getStreamId(),
                deleteStreamClient.getAddr());
    }

    private void handleError(RegionErrorInfo errorInfo) throws ClientException {
        if (errorInfo == null
                || errorInfo.getErrorCode() == null
                || errorInfo.getSingleRegionInfo() == null) {
            LOG.debug("Error info is null.");
            throw new ClientException("receive empty or unknown error msg");
        }
        LOG.error(
                "Error info from Region {}, error info detail: {}",
                errorInfo.getSingleRegionInfo(),
                errorInfo.getErrorCode());
        List<RegionStateManager.SingleRegionInfo> sriList = new ArrayList<>();
        if (errorInfo.getErrorCode().hasNotLeader()) {
            long newStoreId = errorInfo.getErrorCode().getNotLeader().getLeader().getStoreId();
            long newRegionId = errorInfo.getErrorCode().getNotLeader().getRegionId();
            TiRegion oldRegion;
            try {
                oldRegion = this.tiSession.getRegionManager().getRegionById(newRegionId);
            } catch (GrpcException e) {
                LOG.error("Session getRegionId fail during handle region notleader error", e);
                throw new ClientException("GetRegionById fail On NotLeader");
            }
            LOG.warn(
                    String.format(
                            "NotLeader Error with region id [%d] and old store id [%d], new store id [%d]",
                            oldRegion.getId(), oldRegion.getLeader().getStoreId(), newStoreId));
            if (newStoreId == NO_LEADER_STORE_ID) {
                LOG.info(
                        "Received zero store id, from region [{}] try next time",
                        oldRegion.getId());
                throw new ClientException(
                        String.format(
                                "Received zero store id, from region %d try next time",
                                oldRegion.getId()));
            }
            TiRegion newRegion =
                    this.tiSession.getRegionManager().updateLeader(oldRegion, newStoreId);
            if (newRegion == null) {
                LOG.error(
                        "Invalidate region [{}] cache due to cannot find peer when updating leader.Error info is {}",
                        newRegionId,
                        errorInfo.getErrorCode());
                this.tiSession.getRegionManager().onRequestFail(oldRegion);
                throw new ClientException("Update leader failed");
            } else {
                // When switch leader fails or the region changed its region epoch,
                // it would be necessary to re-split task's key range for new region.

                this.tiSession.getRegionManager().invalidateRegion(newRegion);

                if (!oldRegion.getRegionEpoch().equals(newRegion.getRegionEpoch())) {
                    sriList = divideToRegions(errorInfo.getSingleRegionInfo().getSpan(), true);
                } else {
                    TiStore newStore =
                            this.tiSession
                                    .getRegionManager()
                                    .getStoreById(
                                            newRegion.getLeader().getStoreId()); // 不要直接使用newStoreId
                    // 因为updateLeader之后 leader可能变了
                    // update store add.
                    errorInfo.getSingleRegionInfo().getRpcCtx().setTiStore(newStore);
                    String address = newStore.getStore().getAddress();
                    if (newStore.getProxyStore() != null) {
                        address = newStore.getProxyStore().getAddress();
                    }
                    errorInfo.getSingleRegionInfo().getRpcCtx().setAddress(address);
                    LOG.info(
                            "Switch region [{}] to new storeId [{}] to specific leader due to kv return NotLeader.",
                            newRegionId,
                            newRegion.getLeader().getStoreId());
                }
            }
        } else if (errorInfo.getErrorCode().hasEpochNotMatch()) {
            Errorpb.EpochNotMatch epochNotMatch = errorInfo.getErrorCode().getEpochNotMatch();
            for (Metapb.Region region : epochNotMatch.getCurrentRegionsList()) {
                LOG.info("errorinfo region epoch:{}", region.getRegionEpoch().toString());
                LOG.info(
                        "getSingleRegionInfo epoch:{}", errorInfo.getSingleRegionInfo().toString());
                if (region.getId() == errorInfo.getSingleRegionInfo().getVerID().getId()) {
                    if (region.getRegionEpoch().getConfVer()
                            != errorInfo.getSingleRegionInfo().getVerID().getConfVer()
                            || region.getRegionEpoch().getVersion()
                            != errorInfo.getSingleRegionInfo().getVerID().getVer()) {
                        LOG.info("update region epoch");
                        sriList = divideToRegions(errorInfo.getSingleRegionInfo().getSpan(), false);
                    }
                }
            }
        } else if (errorInfo.getErrorCode().hasRegionNotFound()) {
            LOG.info("Kv client region not found error");
//            int maxRetries = 3;
//            long backoffMs = 500;
//            boolean recovered = false;
//            //region可能还没分裂完  立即重试可能会马上再次失败，通过sleep 再次重试
//            for (int i = 1; i <= maxRetries ; i++) {
//                try {
//                    LOG.info("第{}次重试，等待{}ms",i,backoffMs);
//                    Thread.sleep(backoffMs);
//                    //warmUp true 强制刷新
//                    sriList = divideToRegions(errorInfo.getSingleRegionInfo().getSpan(), true);
//
//                    if (sriList !=null || !sriList.isEmpty()){
//                        LOG.info("第{}次重试成功，刷新region数量 {}",i,sriList.size());
//                        recovered = true;
//                        break;
//                    }
//                }catch (InterruptedException e){
//                    Thread.currentThread().interrupt();;
//                    throw new ClientException("kv not found backoff error",e);
//                }catch (Exception e){
//                    LOG.error("第{}次重试失败: {}",i,e.getMessage());
//                }
//                backoffMs *=2;
//            }
//            //还是没有恢复，那么就直接failover
//            if (!recovered){
                throw new ClientException("Kv client region not found error.");
//            }

//            throw new ClientException("Kv client region not found error.");
        } else if (errorInfo.getErrorCode().hasDuplicateRequest()) {
            throw new ClientException("Kv client unreachable error.");
        } else if (errorInfo.getErrorCode().hasCompatibility()) {
            throw new ClientException("tikv reported compatibility error, which is not expected.");
        } else if (errorInfo.getErrorCode().hasClusterIdMismatch()) {
            throw new ClientException(
                    "tikv reported the request cluster ID mismatch error, which is not expected.");
        } else {
            try {
                TiRegion tiRegion =
                        this.tiSession
                                .getRegionManager()
                                .getRegionById(errorInfo.getSingleRegionInfo().getVerID().getId());
                this.tiSession.getRegionManager().onRequestFail(tiRegion);
            } catch (GrpcException e) {
                LOG.error(
                        "Session getRegionId fail during handle Receive empty or unknown error msg:{}",
                        e.getMessage(),
                        e);
            } finally {
                throw new ClientException("Receive empty or unknown error msg.");
            }
        }
        //        Optional<TiTableInfo> tableInfoOptional = getTableInfo(dbName, tableName);
        //        LOG.info("getTableInfo :{}",tableInfoOptional.toString());

        LOG.info(
                "Retry to region request. checkpointTs is [{}].",
                errorInfo.getSingleRegionInfo().getResolvedTs()); // ignore error message

        if (sriList.isEmpty()) {
            sriList.add(errorInfo.getSingleRegionInfo());
        }
        long resolvedTs = sriList.get(0).getResolvedTs();
        long resolvedErrorTs = errorInfo.getSingleRegionInfo().getResolvedTs();
        LOG.info("sriList resolvedTs {} and errorInfo resolvedTs {}", resolvedTs, resolvedErrorTs);

        long minTs = Math.min(sriList.get(0).getResolvedTs(), resolvedErrorTs);
        sriList.forEach(
                singleRegionInfo -> {
                    LOG.info(
                            "Set resolvedTs to {} for region {}",
                            minTs,
                            singleRegionInfo.getVerID().getId());
                    this.tableInfoOptional.ifPresent(
                            tableInfo ->
                                    requestRegionToStore(
                                            singleRegionInfo, tableInfo.getId(), minTs));
                });
    }

    public void detectResolvedTsStuck() {
        if (tsTracker == null) {
            return;
        }
        long resolvedTs = tsTracker.frontier();
        LOG.info("detectResolvedTsStuck resolvedTs:{}, startResolvedTs:{}",resolvedTs,this.startResolvedTs.get());
        // check if the resolvedTs is advancing,
        // If the resolvedTs in Frontier is less than startResolvedTs, it means that the incremental
        // scan has
        // not complete yet. We need to make no decision in this scenario.
        if (resolvedTs <= this.startResolvedTs.get()) {
            return;
        }
        if (resolvedTs == this.lastForwardResolvedTs.get()) {
            LOG.warn(
                    "ResolvedTs stuck detected in puller,lastResolvedTs:{},resolvedTs:{}",
                    this.lastForwardResolvedTs.get(),
                    resolvedTs);
            if (Instant.now().minusSeconds(resolveTsStuckInterval).isAfter(this.lastForwardTime)) {
                throw new ClientException("ResolvedTs stuck detected in puller");
            }
        } else {
            LOG.info("detectResolvedTsStuck step2");
            this.lastForwardResolvedTs.set(resolvedTs);
            this.lastForwardTime = Instant.now();
        }
    }
}

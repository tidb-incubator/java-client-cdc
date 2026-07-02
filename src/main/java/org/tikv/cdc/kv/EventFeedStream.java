package org.tikv.cdc.kv;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ForwardingExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.model.RegionStatefulEvent;
import org.tikv.common.util.IDAllocator;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.shade.io.grpc.MethodDescriptor;
import org.tikv.shade.io.grpc.stub.StreamObserver;
import org.tikv.shade.io.netty.channel.MultithreadEventLoopGroup;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EventFeedStream implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(EventFeedStream.class);
    private static final Pattern CANCEL_REASON_PATT =
            Pattern.compile("rpc error: code = (\\w+) desc = (.*)");
    private final long maxInflightUnits = 3000L;
    private final int initialRequests= 2;
    private final AtomicLong inflightUnits = new AtomicLong(0);
    private static final MethodDescriptor<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
            METHOD_EVENT_FEED = ChangeDataGrpc.getEventFeedMethod();
    private static final Exception CANCEL_EXCEPTION = new CancellationException();

    private final GRPCClient client;

    private final String storeAddr;
    private final Long storeId;
    private final long streamId;
    private final Instant createTime;

    private final RegionStateManager.SyncRegionFeedStateMap regions;

    private RegionWorker regionWorker;

    private final Executor observerExecutor; // "parent" executor
    private final Executor eventLoop; // serialized


    private final ScheduledExecutorService dedicatedScheduler ;
//    private final ScheduledExecutorService dedicatedWorker ;
    private final AtomicReference<ReceiveEvent> receiveEvent = new AtomicReference<>();

    protected AtomicBoolean closed = new AtomicBoolean(false);

    // 使用专用锁对象代替this锁，减少锁粒度
    private final Object streamLock = new Object();
    private StreamObserver<Cdcpb.ChangeDataRequest> requestStream;

    private final ConcurrentLinkedQueue<RegionStatefulEvent> resolveTsPool =
            new ConcurrentLinkedQueue<>();

    public EventFeedStream(String storeAddr, long storeId, RPCContext rpcContext, long rate) {
        this.streamId = IDAllocator.allocateStreamClientID();
        this.dedicatedScheduler = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("stream-"+streamId+"-scheduler-%d").build());
//        this.dedicatedWorker = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("stream-"+streamId+"-worker-%d").build());
        this.client = buildGrpcClient(rpcContext, rate);
        this.storeAddr = storeAddr;
        this.storeId = storeId;
        this.regions = new RegionStateManager.SyncRegionFeedStateMap();
        this.createTime = Instant.now();

        //这里是GrpcClient中的workergroup
        this.observerExecutor = this.client.getResponseExecutor();
        //这里负责流的关闭和开启？  eventloop
        this.eventLoop = GRPCClient.serialized(client.getInternalExecutor());
        this.resolveTsPool.add(
                new RegionStatefulEvent.Builder()
                        .setResolvedTsEvent(new RegionStatefulEvent.ResolvedTsEvent())
                        .build());
    }
    private long estimateUnits(Cdcpb.ChangeDataEvent event){
        long units = 0;
        if (event.getEventsList() !=null){
            for (Cdcpb.Event e : event.getEventsList()){
                if (e.hasEntries()){
                    units +=Math.max(1,e.getEntries().getEntriesCount());
                }else {
                    units += 1;
                }
            }
        }
        if (event.hasResolvedTs()){
            units+=1;
        }
        return Math.max(1,units);
    }

    private void onEventStart(long units){
        long now = inflightUnits.addAndGet(units);
        LOG.debug("event start ,streamId={} , storeAddr= {}, units={},inflightunits={}",streamId,storeAddr,units,now);

    }

    private void onEventDone(long units){
        long now = inflightUnits.addAndGet(-units);
        if (now <0){
            inflightUnits.set(0);
            now = 0;
        }

        if (!closed.get()&&now<maxInflightUnits){
            client.requestManual(1);
            LOG.info("event end ,streamId={},storeAddr = {},inflightUnits={}",streamId,storeAddr,now);
        }else {
            LOG.info("skip request!");
        }
    }

    public boolean StartReceiver(Cdcpb.ChangeDataRequest request) {
        //因为是缓存的stream，这里startReceiver
        if (closed.get()) {
            LOG.info("startReceiver:{}", closed.get());
            throw new IllegalStateException("closed");
        }
        LOG.info("StartReceiver step1 regionId:{}",request.getRegionId());
        //为当前这个特定的GRPC的stream创建一个会话管理状态，rEVENT  observerexecutor负责执行业务逻辑？
        final ReceiveEvent rEvent = new ReceiveEvent(request, observerExecutor);
        LOG.info("StartReceiver step2");
        Cdcpb.ChangeDataRequest createReq = rEvent.firstCreateChangeDateRequest();
        synchronized (streamLock) {
            LOG.info("StartReceiver step3");
            StreamObserver<Cdcpb.ChangeDataRequest> requestStream = getRequestStream();
            if (requestStream == null) { // todo it seems some circle.
                LOG.info("EventFeedStream closed");
                closed.getAndSet(true);
                return false;
            }
            LOG.info("Stream {} ,Send change request is {} ,  streamType:{}", getStreamId(), createReq, requestStream.getClass().getName());
            requestStream.onNext(createReq);
            //测试
//            client.requestManual(1);
        }
        // 在锁外更新receiveEvent，减少锁持有时间
        this.receiveEvent.set(rEvent);
        closed.getAndSet(false);
        return true;
    }

    final class ReceiveEvent {
        private final long regionId;
        private final Cdcpb.ChangeDataRequest request;
        private final Executor receiveExecutor;
        boolean finished;

        long receivedSize = 0L;
        private final AtomicLong currentCheckpointTs = new AtomicLong();

        ReceiveEvent(Cdcpb.ChangeDataRequest request, Executor parentExecutor) {
            this.request = request;
            this.regionId = request.getRegionId();
            // bounded for back-pressure
            this.receiveExecutor = GRPCClient.serialized(parentExecutor);
        }

        public Cdcpb.ChangeDataRequest firstCreateChangeDateRequest() {
            return request;
        }

        public Cdcpb.ChangeDataRequest newCreateChangeDateRequest() {
            return request.toBuilder()
                    .setRequestId(IDAllocator.allocateRequestID())
                    .setCheckpointTs(this.currentCheckpointTs.get())
                    .build();
        }

        public long getRegionId() {
            return regionId;
        }

        // null => cancelled (non-error)
        public void publishCompletionEvent(final Exception err) {
            receiveExecutor.execute(
                    () -> {
                        try {
                            LOG.error("Receive observer onCompleted/onError with error.", err);
                            if (err == null) {
                                responseObserver.onCompleted();
                            } else {
                                responseObserver.onError(err);
                            }
                        } catch (RuntimeException e) {
                            LOG.error("Receive observer onCompleted/onError threw", e);
                        }
                    });
        }

        @GuardedBy("eventLoop")
        public void processEvent(final Cdcpb.ChangeDataEvent event) {

            long units = EventFeedStream.this.estimateUnits(event);
            EventFeedStream.this.onEventStart(units);
            try {
                LOG.info("current sleep eventfeedstream:{}",streamId);
                receiveExecutor.execute(
                        () -> {
                            long size = event.getSerializedSize();
                            try{
                                if (size > 12 * 1024 * 1024) {
                                    int regionCount = 0;
                                    if (event.hasResolvedTs()) {
                                        regionCount = event.getResolvedTs().getRegionsCount();
                                    }
                                    LOG.warn(
                                            "change data event size too large. size:{},resolvedRegionCount:{}",
                                            size,
                                            regionCount);
                                }
                                if (event.getEventsList() != null && !event.getEventsList().isEmpty()) {
                                    LOG.debug("Send event list number is {}", receivedSize);
                                    if (event.getEventsList().get(0).hasEntries()) {
                                        long commitTs =
                                                event.getEventsList()
                                                        .get(0)
                                                        .getEntries()
                                                        .getEntries(0)
                                                        .getCommitTs();
                                        if (currentCheckpointTs.get() < commitTs) {
                                            currentCheckpointTs.getAndSet(commitTs);
                                        }
                                    }
                                    LOG.debug("Metric:commit ts :{}", currentCheckpointTs.get());
                                    receivedSize = receivedSize + 1;
                                    sendRegionChangeEvent(event.getEventsList(), regionWorker);
                                }

                                if (event.hasResolvedTs()) {
                                    LOG.trace(
                                            "stream {},ts {}，regions {}",
                                            regionWorker.getStream().getStreamId(),
                                            event.getResolvedTs().getTs(),
                                            event.getResolvedTs().getRegionsList());
                                    sendResolveTs(event.getResolvedTs(), regionWorker);
                                }
                            }finally {
                                EventFeedStream.this.onEventDone(units);
                            }
                        });
            }catch (RuntimeException e){
                LOG.error("processEvent error on RuntimeException",e);
                EventFeedStream.this.onEventDone(units);
                throw e;
            }
        }
    }

    public StreamObserver<Cdcpb.ChangeDataRequest> getRequestStream() {
        LOG.info("EventFeedStream getRequestsream closed :{}", closed.get());
        if (closed.get()) return null;
        synchronized (streamLock) {
            if (requestStream == null) {
                LOG.debug("Event feed stream starting");
                //这里eventLoop负责将GRPCclient过来的数据排序？ responseObserver进行处理？
                requestStream = client.callStream(METHOD_EVENT_FEED, responseObserver, eventLoop);
            }
            return requestStream;
        }
    }

    public void closeRequestStreamIfNoEvents() {
        synchronized (streamLock) {
            if (requestStream != null) {
                requestStream.onError(CANCEL_EXCEPTION);
                LOG.info("Watch stream cancelled due to there being no active watches");
                requestStream = null;
            }
        }
    }

    protected final GRPCClient.ResilientResponseObserver<
            Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
            responseObserver =
            new GRPCClient.ResilientResponseObserver<
                    Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>() {

                @Override
                public void onNext(Cdcpb.ChangeDataEvent event) {
                    processResponse(event);
                }

                @Override
                public void onError(Throwable t) {
                    LOG.error("onError called for watch request stream", t);
                    if (closed.get()
                            || GRPCClient.causedBy(t, CancellationException.class)) {
                        return;
                    }
                    // 避免使用this锁，直接检查closed状态
                    onReplacedOrFailed(
                            null,
                            t instanceof Exception
                                    ? (Exception) t
                                    : new RuntimeException(t));
                }

                @Override
                public void onCompleted() {
                    LOG.warn("onCompleted called for event feed stream request stream");
                }

                @Override
                public void onEstablished() {
                    // nothing to do here
                    LOG.warn(
                            "onEstablished called for event feed request stream.stream id :{},store address:{}",
                            streamId,
                            storeAddr);
                    inflightUnits.set(0);
                    client.requestManual(initialRequests);
                    LOG.info("initial request granted,stream id :{},store address:{}",streamId,
                            storeAddr);
                }

                @Override
                public void onReplaced(
                        StreamObserver<Cdcpb.ChangeDataRequest> newStreamRequestObserver) {
                    if (!closed.get()) {
                        LOG.info(
                                "onReplaced called for event feed request stream {}",
                                (newStreamRequestObserver == null
                                        ? " with newReqStream == null"
                                        : ""));
                    }
                    onReplacedOrFailed(newStreamRequestObserver, null);
                }

                void onReplacedOrFailed(
                        StreamObserver<Cdcpb.ChangeDataRequest> newReqStream,
                        Exception err) {
                    ReceiveEvent currentEvent;
                    synchronized (EventFeedStream.this) {
                        requestStream = newReqStream;

                        currentEvent = receiveEvent.get();
                        if (currentEvent != null) {
                            receiveEvent.set(null);
                        }
                    }
                    LOG.info("onReplaceOrFailed resend only regionId = {}",currentEvent.getRegionId());
                    if (currentEvent != null && !currentEvent.finished) {
                        if (newReqStream != null && !closed.get()) {
                            // resend
                            Cdcpb.ChangeDataRequest newReq =
                                    currentEvent.newCreateChangeDateRequest();
                            LOG.info("newReq:{}",newReq.toString());
                            synchronized (EventFeedStream.this) {
                                if (!closed.get()) {
                                    try {
                                        LOG.info("start newReq");
                                        requestStream.onNext(newReq);
                                        receiveEvent.set(currentEvent);
                                        client.requestManual(1);
                                        // with no close if request success.
                                        return;
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Failed to send request on new stream", e);
                                    }
                                }
                            }
                        }
                        LOG.info("finish onRplacedOrFailed newReq");
                        // Stream rebuild failed or closed, mark event complete
                        currentEvent.finished = true;
                        currentEvent.publishCompletionEvent(
                                err != null ? err : new IOException("Stream closed"));
                        LOG.info("onReplacedOrFailed");
//                                try{
//                                    LOG.info("onReplacedOrFailed step 1");
//                                    close();
//                                }catch (IOException e){
//                                    LOG.error("failed to close eventfeedstream",e);
//                                }
                    }

                    // close request stream if no events
                    if (newReqStream != null && !closed.get()) {
                        // Check if the client's internal executor is still available before
                        // attempting to close
                        ScheduledExecutorService internalExecutor =
                                client.getInternalExecutor();
                        if (internalExecutor != null
                                && !internalExecutor.isShutdown()
                                && !internalExecutor.isTerminated()) {
                            closeRequestStreamIfNoEvents();
                        } else {
                            LOG.warn(
                                    "Cannot close request stream as internal executor is terminated");
                        }
                    }
                }
            };

    protected void processResponse(Cdcpb.ChangeDataEvent event) {
        ReceiveEvent re = this.receiveEvent.get();
        if (re == null) {
            LOG.error("State error: received unexpected watch create response: " + event);
            closeRequestStreamIfNoEvents();
            return;
        }
        re.processEvent(event);
    }

    public boolean getIsCanceled() {
        return closed.get();
    }

    public void cancelStream(int delay) throws IOException, InterruptedException {
        close();
        Thread.sleep(delay * 1000);
    }

    private GRPCClient buildGrpcClient(RPCContext rpcContext, long rate) {
        LOG.info("buildGrpcClient Rate :{} , streamId:{}", rate,streamId);
        //传入已经建立好的ManagedChannel、
        // ScheduledExecutorService ses, bossgroup  负责底层数据通信
        //   Executor userExecutor,  workerGroup负责数据传输
        //这里eventfeedstream 和 grpcclient是 一对一的关系， 但是 由于都是调用rpcContext中相关的bossGroup和workerGroup，这里又是多对一的

        return new GRPCClient(
                rpcContext.getChannel(),
                dedicatedScheduler,
                rpcContext.getChannelFactory().getWorkerGroup(),
                rate,streamId);

//        return new GRPCClient(
//                rpcContext.getChannel(),
//                new SharedScheduledExecutorService(
//                        rpcContext.getChannelFactory().getBossEventLoopGroup()),
//                rpcContext.getChannelFactory().getWorkerGroup(),
//                rate,streamId);
    }

    public String getAddr() {
        return storeAddr;
    }

    public Long getStoreId() {
        return storeId;
    }

    public long getStreamId() {
        return streamId;
    }

    public RegionStateManager.SyncRegionFeedStateMap getRegions() {
        return regions;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public synchronized void setRegionWorker(RegionWorker worker) {
        if (this.regionWorker != null) {
            throw new IllegalStateException("A RegionWorker is already bound to this stream.");
        }
        this.regionWorker = worker;
    }

    public RegionWorker getRegionWorker() {
        return regionWorker;
    }

    @Override
    public void close() throws IOException {
        LOG.info("eventfeedsteram closed ");
        if (closed.get()) {
            return;
        }
        eventLoop.execute(
                () -> {
                    if (!closed.get())
                        synchronized (EventFeedStream.this) {
                            if (closed.get()) {
                                return;
                            }
                            closed.compareAndSet(false, true);
                            if (requestStream != null) {
                                //
                                // requestStream.onError(CANCEL_EXCEPTION);
                                requestStream.onCompleted();
                                requestStream = null;
                            }
                            responseObserver.onReplaced(null);
                        }
                });
        if (regionWorker != null) {
            regionWorker.evictAllRegions();
            regionWorker = null; // 可选：关闭 worker
        }
    }

    private synchronized void sendRegionChangeEvent(List<Cdcpb.Event> events, RegionWorker worker) {
        List<List<RegionStatefulEvent>> regionStatefulEeventList =
                IntStream.range(0, worker.getWorkerConcurrency())
                        .mapToObj(i -> new ArrayList<RegionStatefulEvent>())
                        .collect(Collectors.toList());
        for (Cdcpb.Event event : events) {
            RegionStateManager.RegionFeedState state = worker.getRegionState(event.getRegionId());
            //      boolean valid = true;
            if (state != null) {
                if (state.getRequestID() < event.getRequestId()) {
                    LOG.debug(
                            "region state entry will be replaced because received message of newer requestID.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                } else if (state.getRequestID() > event.getRequestId()) {
                    //观察是不是因为这个把数据丢失了
                    LOG.debug(
                            "drop event due to event belongs to a stale request.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                    continue;
                }
                if (state.isStale()) {
                    LOG.warn(
                            "drop event due to region feed is stopped.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                    continue;
                }
            } else {
                // Firstly load the region info.
                RegionStateManager.RegionFeedState newState =
                        worker.getStream().getRegions().takeByRequestID(event.getRequestId());
                if (newState == null) {
                    LOG.warn(
                            "drop event due to region feed is removed.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                    continue;
                }
                newState.start();
                state = newState;
                worker.setRegionState(event.getRegionId(), newState);
            }
            if (event.hasError()) {
                LOG.error(
                        "event feed receives a region error.regionId {}, oldRequestId {}, requestId {}, add {},streamId {}，error is {}",
                        event.getRegionId(),
                        state.getRequestID(),
                        event.getRequestId(),
                        worker.getStream().getAddr(),
                        worker.getStream().getStreamId(),
                        event.getError());
            }
            int slot = worker.inputCalcSlot(event.getRegionId());
            // build stateful event;
            regionStatefulEeventList
                    .get(slot)
                    .add(
                            new RegionStatefulEvent.Builder()
                                    .setEvent(event)
                                    .setRegionFeedState(state)
                                    .setRegionId(event.getRegionId())
                                    .build());
        }
        for (List<RegionStatefulEvent> rsevents : regionStatefulEeventList) {
            if ((!rsevents.isEmpty())) {
                worker.processEvents(rsevents);
            }
        }
    }

    private void sendResolveTs(Cdcpb.ResolvedTs resolvedTs, RegionWorker worker) {
        List<RegionStatefulEvent> regionStatefulEvents =
                IntStream.range(0, worker.getWorkerConcurrency())
                        .mapToObj(i -> new RegionStatefulEvent()) // 假设有一个无参构造函数
                        .collect(Collectors.toList());
        for (int i = 0; i < worker.getWorkerConcurrency(); i++) {
            int buffLen = resolvedTs.getRegionsList().size() / worker.getWorkerConcurrency() * 2;
            RegionStatefulEvent rse = this.resolveTsPool.poll();
            if (rse == null) {
                rse = new RegionStatefulEvent();
                this.resolveTsPool.add(rse);
            }
            rse.getResolvedTsEvent().setResolvedTs(resolvedTs.getTs());
            rse.getResolvedTsEvent().setRegions(new ArrayList<>(buffLen));
            regionStatefulEvents.set(i, rse);
        }
        for (long regionID : resolvedTs.getRegionsList()) {
            RegionStateManager.RegionFeedState state = worker.getRegionState(regionID);
            if (state != null) {
                int slot = worker.inputCalcSlot(regionID);
                LOG.trace("Assign slot {} to region {}", slot, regionID);
                RegionStatefulEvent rse = regionStatefulEvents.get(slot);
                rse.getResolvedTsEvent().setRegions(Collections.singletonList(state));
                rse.setRegionId(regionID);
                regionStatefulEvents.set(slot, rse);
            } else {
                LOG.warn("Region {} is not found in region state manager, skip it.", regionID);
            }
        }
        for (RegionStatefulEvent rse : regionStatefulEvents) {
            if (!rse.getResolvedTsEvent().getRegions().isEmpty()) {
                worker.processEvents(Lists.newArrayList(rse));
            }
        }
    }

    /** Wrapper to prevent direct shutdown */
    static final class SharedScheduledExecutorService extends ForwardingExecutorService
            implements ScheduledExecutorService {
        private final ScheduledExecutorService delegate;

        SharedScheduledExecutorService(ScheduledExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        protected ExecutorService delegate() {
            return delegate;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return delegate.schedule(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return delegate.schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            return delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException(
                    "Cannot be shut down directly, close EventFeedStream instead");
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException(
                    "Cannot be shut down directly, close EventFeedStream instead");
        }
    }
}

package org.tikv.cdc.kv;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.io.grpc.CallOptions;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.MethodDescriptor;
import org.tikv.shade.io.grpc.Status;
import org.tikv.shade.io.grpc.internal.SerializingExecutor;
import org.tikv.shade.io.grpc.stub.ClientCallStreamObserver;
import org.tikv.shade.io.grpc.stub.ClientCalls;
import org.tikv.shade.io.grpc.stub.ClientResponseObserver;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GRPCClient {
    private static final Logger LOG = LoggerFactory.getLogger(GRPCClient.class);
    private final ManagedChannel channel;
    private final long streamId;
    // data read limit for read data.
    protected RateLimiter dataReadLimiter;

    // modified only by reauthenticate() method
    private CallOptions callOptions = CallOptions.DEFAULT; // volatile tbd - lazy probably ok
    protected final ScheduledExecutorService ses;
    private  volatile  ResilientBiDiStream<?,?> activeStream;
    protected final Executor userExecutor;

    public GRPCClient(
            ManagedChannel channel,
            ScheduledExecutorService ses,
            Executor userExecutor,
            long rate,
            long streamId) {

        this.channel = Preconditions.checkNotNull(channel, "channel is null.");
        this.ses = ses;
        this.streamId = streamId;
        this.userExecutor = userExecutor;
        Preconditions.checkArgument(rate > 0, "Rate must be positive.");
        this.dataReadLimiter = RateLimiter.create(rate);
    }

    protected CallOptions getCallOptions() {
        return callOptions;
    }

    public static boolean causedBy(Throwable t, Class<? extends Throwable> exClass) {
        return t != null
                && (exClass.isAssignableFrom(t.getClass()) || causedBy(t.getCause(), exClass));
    }

    public static boolean isConnectException(Throwable t) {
        return causedBy(t, ConnectException.class) || causedBy(t, NoRouteToHostException.class);
    }

    public static Status.Code codeFromThrowable(Throwable t) {
        return Status.fromThrowable(t).getCode(); // fromThrowable won't return null
    }

    protected boolean retryableStreamError(Throwable error) {
        return (Status.fromThrowable(error).getCode() != Status.Code.INVALID_ARGUMENT
                && !causedBy(error, Error.class));
    }

    public Executor getResponseExecutor() {
        return userExecutor;
    }

    /** Care should be taken not to use this executor for any blocking or CPU intensive tasks. */
    public ScheduledExecutorService getInternalExecutor() {
        return ses;
    }

    private static final Class<? extends Executor> GSE_CLASS =
            MoreExecutors.newSequentialExecutor(directExecutor()).getClass();

    public static Executor serialized(Executor parent) {
        return parent instanceof SerializingExecutor || parent.getClass() == GSE_CLASS
                ? parent
                : new SerializingExecutor(parent);
    }

    public <ReqT, RespT> StreamObserver<ReqT> callStream(
            MethodDescriptor<ReqT, RespT> method,
            ResilientResponseObserver<ReqT, RespT> respStream,
            Executor responseExecutor) {
        //respStream 指的是responseObserver   responseExecutor指的是eventLoop
        ResilientBiDiStream<ReqT,RespT> stream = new ResilientBiDiStream<>(method, respStream, responseExecutor);
        this.activeStream =stream;
        return stream.start();
    }

    public void requestManual(int n){
        ResilientBiDiStream<?,?> stream = activeStream;
        if (stream != null){
            LOG.info("requestManual success");
            stream.request(n);
        }else {
            LOG.error("no active stream found to send manual request.");
        }
    }

    public interface ResilientResponseObserver<ReqT, RespT> extends StreamObserver<RespT> {
        /**
         * Called once initially, and once after each {@link #onReplaced(StreamObserver)}, to
         * indicate the corresponding (sub) stream is successfully established
         */
        void onEstablished();

        /**
         * Indicates the underlying stream failed and will be re-established. There is no guarantee
         * that any requests sent to the current request stream have been delivered, the provided
         * stream should be used in its place to send all subsequent requests, including
         * re-submissions if necessary. Any subsequent {@link #onEstablished()} or {@link
         * #onNext(Object)} calls received will be responses from this <b>new</b> stream, it's
         * guaranteed that there will be no more from the prior stream.
         *
         * @param newStreamRequestObserver
         */
        void onReplaced(StreamObserver<ReqT> newStreamRequestObserver);
    }

    @SuppressWarnings("rawtypes")
    private static final StreamObserver<?> EMPTY_STREAM =
            new StreamObserver() {
                @Override
                public void onCompleted() {}

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onNext(Object value) {}
            };

    protected static void closeStream(StreamObserver<?> stream, Throwable err) {
        if (err == null) {
            stream.onCompleted();
        } else {
            stream.onError(err);
        }
    }

    @SuppressWarnings("unchecked")
    protected static <ReqT> StreamObserver<ReqT> emptyStream() {
        return (StreamObserver<ReqT>) EMPTY_STREAM;
    }

    final class ResilientBiDiStream<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;
        private final ResilientResponseObserver<ReqT, RespT> respStream;
        private final Executor responseExecutor;
        private volatile  ClientCallStreamObserver<ReqT> currentCallObserver;
        // null if !sendViaEventLoop
        private final Executor requestExecutor;

        // accessed only from response thread and retry task scheduled
        // from the onError message (prior to stream being reestablished)
        private CallOptions sentCallOptions;
        private int errCounter = 0;

        // provided to user, buffers and wraps real req stream when active.
        // field accessed only from response thread
        private RequestSubStream userReqStream;

        // finished reflects *user* closing stream via terminal method (not incoming stream closure)
        // error == null indicates complete versus failed when finished == true
        // modified only by response thread
        private boolean finished;
        private Throwable error;

        /**
         * @param method
         * @param respStream
         */
        ResilientBiDiStream(
                MethodDescriptor<ReqT, RespT> method,
                ResilientResponseObserver<ReqT, RespT> respStream,
                Executor responseExecutor) {
            //respStream 指的是responseObserver   responseExecutor指的是eventLoop
            this.method = method;
            this.respStream = respStream;
            this.responseExecutor =
                    serialized(responseExecutor != null ? responseExecutor : userExecutor);
            //ses指的是前面的bossGroup
            this.requestExecutor = serialized(ses);
        }

        public void request(int n){
            ClientCallStreamObserver<ReqT> obs = this.currentCallObserver;
            if(obs ==null || n<=0){
                return;
            }
            try {
                if (obs.isReady()){
                    obs.request(n);
                }else {
                    LOG.debug("request skipped because observer is not ready n={}",n);
                }
            }catch (Exception e){
                LOG.error("request failed",e);
            }
        }


        // must only be called once - enforcement logic omitted since private
        StreamObserver<ReqT> start() {
            RequestSubStream firstStream = new RequestSubStream();
            userReqStream = firstStream;
            //只有这里执行后，才会去执行beforestart 一个stream一旦执行了refreshbackingStream，这里就会去新建，respWrapper只实例化一次，但是后续执行refreshbackingstream后，会重新绑定，执行beforeStart和isReady
            responseExecutor.execute(this::refreshBackingStream);
            return firstStream;
        }

        class RequestSubStream implements StreamObserver<ReqT> {
            // lifecycle: null -> real stream -> EMPTY_STREAM
            private volatile StreamObserver<ReqT> grpcReqStream; // only modified by response thread
            // grpcReqStream non-null => preConnectBuffer null


            private Queue<ReqT> preConnectBuffer;

            // called by user thread
            @Override
            public void onNext(ReqT value) {
                if (finished) {
                    return; // illegal usage
                }
                StreamObserver<ReqT> rs = grpcReqStream;
                if (rs == null)
                    synchronized (this) {
                        rs = grpcReqStream;
                        if (rs == null) {
                            if (preConnectBuffer == null) {
                                preConnectBuffer = new ArrayDeque<>(8);
                            }
                            preConnectBuffer.add(value);
                            return;
                        }
                    }

                if (requestExecutor == null) {
                    sendOnNext(rs, value); // (***)
                } else {
                    final StreamObserver<ReqT> rsFinal = rs;
                    requestExecutor.execute(() -> sendOnNext(rsFinal, value));
                }
            }

            private void sendOnNext(StreamObserver<ReqT> reqStream, ReqT value) {
                try {
                    reqStream.onNext(value);
                } catch (IllegalStateException ise) {
                    // this is possible and ok if the stream was already closed
                    if (grpcReqStream != emptyStream()) throw ise;
                }
            }

            // called by user thread
            @Override
            public void onError(Throwable t) {
                onFinish(t);
            }

            // called by user thread
            @Override
            public void onCompleted() {
                onFinish(null);
            }

            // called from response thread  在readyHandler里面调用，
            boolean established(StreamObserver<ReqT> stream) {
                StreamObserver<ReqT> curStream = grpcReqStream;
                if (curStream == null)
                    synchronized (this) {
                        Queue<ReqT> pcb = preConnectBuffer;
                        if (pcb != null) {
                            for (ReqT req; (req = pcb.poll()) != null; ) {
                                stream.onNext(req);
                            }
                            preConnectBuffer = null;
                        }
                        initialReqStream = null;
                        if (finished) {
                            LOG.debug("On established stream, grpc ReqStream is emptyStream.");
                            grpcReqStream = emptyStream();
                        } else {
                            grpcReqStream = stream;
                            return true;
                        }
                    }
                else if (stream == curStream) {
                    return false;
                }

                // here either finished or it's an unexpected new stream
                if (!finished) {
                    LOG.info(
                            "Closing unexpected new stream of method {}",
                            method.getFullMethodName());
                }
                closeStream(stream, error);
                return false;
            }

            boolean isEstablished() {
                return grpcReqStream != null;
            }

            // called by user thread
            private void onFinish(Throwable err) {
                channel.shutdown();
                if (finished) {
                    return; // shouldn't be called more than once anyhow
                }
                responseExecutor.execute(
                        () -> {
                            if (finished) {
                                return;
                            }
                            if (err == null) {
                                error = err;
                                finished = true;
                            }
                            userReqStream.close(err, true);
                        });
            }

            // called from grpc response thread
            void discard(Throwable err) {
                StreamObserver<ReqT> curStream = grpcReqStream, empty = emptyStream();
                if (curStream == empty) {
                    return;
                }
                if (curStream == null)
                    synchronized (this) {
                        grpcReqStream = empty;
                        preConnectBuffer = null;
                    }
                else {
                    // TODO this *could* overlap with an in-progress
                    //   onNext (***) above in the sendViaEventLoop == false case, but unlikely
                    // For now, delay sending the close to further minimize the chance
                    close(err, false);
                }
            }

            // called from grpc response thread
            void close(Throwable err, boolean fromUser) {
                StreamObserver<ReqT> curStream = grpcReqStream, empty = emptyStream();
                if (curStream == null || curStream == empty) {
                    return;
                }
                grpcReqStream = empty;
                if (fromUser) {
                    closeStream(curStream, err);
                } else {
                    Runnable closeTask = () -> closeStream(curStream, err);
                    if (requestExecutor != null) {
                        requestExecutor.execute(closeTask);
                    } else {
                        ses.schedule(closeTask, 400, MILLISECONDS);
                    }
                }
            }
        }
        /*
         * We assume the caller (grpc) abides by StreamObserver contract
         */
        public final StreamObserver<RespT> respWrapper =
                new ClientResponseObserver<ReqT, RespT>() {
                    @Override
                    public void beforeStart(ClientCallStreamObserver<ReqT> rs) {
                        LOG.info("beforeStart");
                        rs.disableAutoRequestWithInitial(0);
                        //物理流给 currentCallObserver 后续的手动请求通过currentCallObserver进行
                        currentCallObserver = rs;
                        rs.setOnReadyHandler(
                                () -> {
                                    // called from grpc response thread
                                    if (rs.isReady()) {
                                        LOG.info("beforeStart established");
                                        boolean notify = userReqStream.established(rs);
                                        if (notify) {
                                            LOG.info("beforeStart request");
                                            //这里调用了respStream.onEstablished
                                            respStream.onEstablished();
                                        }
                                    }
                                });
                    }
                    // called from grpc response thread
                    @Override
                    public void onNext(RespT value) {
                        long size = ((Cdcpb.ChangeDataEvent) value).getSerializedSize();
                        int sizeKB = (int) (size/1024.0);
                        sizeKB = sizeKB==0?1:sizeKB;
                        try {
                            //计算这个dataEvent中有多少数据，这里分为prewrite和commit 同时有索引和数据，那么就是4倍TPS
                            if (dataReadLimiter != null){
                                double waitTime = dataReadLimiter.acquire(sizeKB);
                                LOG.info("waitTime:{},streamId:{}",waitTime,streamId);
                            }
                            LOG.info("current data sieze:{} kb",sizeKB);
                            respStream.onNext(value);

                        }catch (Throwable e){
                            LOG.error("GRPCclient onNext",e);
                        }
//                        finally {
//                            currentCallObserver.request(1);
//                        }
                    }



//                    @Override
//                    public void onNext(RespT value) {
//
//
//                        try {
//                            long size = ((Cdcpb.ChangeDataEvent) value).getSerializedSize();
//                            LOG.info("current size:{}",size);
//                            respStream.onNext(value);
//                        }catch (Throwable e) {
//                            LOG.error("GRPCclient onNext", e);
//                        }
////                        }finally {
////                            if (currentCallObserver != null){
////                                if (dataReadLimiter != null && value instanceof Cdcpb.ChangeDataEvent){
////                                    notArriveRequest.decrementAndGet();
//////                                    ((Cdcpb.ChangeDataEvent) value).getSerializedSize()
////                                    Cdcpb.ChangeDataEvent ev = (Cdcpb.ChangeDataEvent) value;
////                                    int rowCount = ev.getEventsList().stream().mapToInt(e->e.getEntries().getEntriesCount()).sum();
////                                    rowCount = rowCount > 0 ? rowCount : 1;
////                                    //如果在dataReadLimiter中获取不到，那就延迟  else 直接request
////                                    if (!dataReadLimiter.tryAcquire(rowCount)){
////                                        double rate = dataReadLimiter.getRate();
////                                        long delayMs = (long)((rowCount/ rate)*1000);
//////                                        LOG.info("GRPC limiter delay 1, delayMs {} ms",delayMs);
//////                                        delayMs = Math.min(Math.max(delayMs,1000),2000);
////                                        LOG.info("GRPC limiter delay 2, delayMs {} ms",delayMs);
////                                        ses.schedule(()-> request(1),delayMs,MILLISECONDS);
////                                    }else {
////                                        request(1);
////                                    }
////                                }else {
////                                    notArriveRequest.decrementAndGet();
////                                    request(1);
////                                }
////                            }
////                        }
//                    }
                    // called from grpc response thread
                    @Override
                    public void onError(Throwable t) {
                        //主要是failover后的处理逻辑，重启 这里应该是不影响failover后的数据的
                        boolean finalError;
                        if (finished) {
                            finalError = true;
                        } else {
                            finalError = !retryableStreamError(t);
                        }
                        if (!finalError) {
                            int errCount = ++errCounter;
                            LOG.error(
                                    "Retryable onError #{} on underlying stream of method {}",
                                    errCount,
                                    method.getFullMethodName(),
                                    t);
                            //存储的当前请求流，
                            RequestSubStream userStreamBefore = userReqStream;

                            //判断当前流是否已经建立，如果已经建立，清空，在refreshbackingstream中重新绑定
                            if (userStreamBefore.isEstablished()) {
                                userReqStream = new RequestSubStream();
                                userStreamBefore.discard(null);
                                // must call onReplaced prior to refreshing the stream, otherwise
                                // the response observer may be called with responses from the
                                // new stream prior to onReplaced returning
                                //替换
                                respStream.onReplaced(userReqStream);

                            } else if (initialReqStream != null) {
                                // else no need to replace user stream, but cancel outbound stream
                                initialReqStream.onError(t);
                                initialReqStream = null;
                            }

                            // delay stream retry using backoff/jitter strategy
                            LOG.info(
                                    "Delay stream retry onError #{} on underlying stream of method {}",
                                    errCount,
                                    method.getFullMethodName(),
                                    t);
                            //延迟refreshBackingStream绑定
                            if (!ses.isShutdown() && !ses.isTerminated()) {
                                ses.schedule(
                                        ResilientBiDiStream.this::refreshBackingStream,
                                        // skip attempt in rate-limited case (errCount <=1)
                                        delayAfterFailureMs(Math.max(errCount, 2)),
                                        MILLISECONDS);
                            } else {
                                LOG.warn(
                                        "ScheduledExecutorService is terminated, cannot schedule retry for stream");
                            }
                        } else {

                            sentCallOptions = null;
                            userReqStream.discard(t);
                            respStream.onError(t);
                        }
                    }
                    // called from grpc response thread
                    @Override
                    public void onCompleted() {
                        if (!finished) {
                            LOG.warn(
                                    "Unexpected onCompleted received for stream of method {}",
                                    method.getFullMethodName());
                            // TODO(maybe) reestablish stream in this case?
                        }
                        sentCallOptions = null;
                        userReqStream.discard(null);
                        respStream.onCompleted();
                    }
                };
        // called only from:
        // - grpc response thread
        // - scheduled retry (no active stream)
        private void refreshBackingStream() {
            if (finished) {
                return;
            }
            LOG.debug("Refreshing backing stream");
            try {
                CallOptions callOpts = getCallOptions();
                sentCallOptions = callOpts;
                callOpts = callOpts.withExecutor(responseExecutor);
                if (channel.isTerminated()) {
                    LOG.debug("Channel is terminated, resetting connect backoff.");
                    channel.resetConnectBackoff();
                }

                //只有这里执行后，才会去执行beforestart 一个stream一旦执行了refreshbackingStream，这里就会去新建，respWrapper只实例化一次，但是后续执行refreshbackingstream后，会重新绑定，执行beforeStart和isReady
                initialReqStream =
                        ClientCalls.asyncBidiStreamingCall(
                                channel.newCall(method, callOpts), respWrapper);
            } catch (Exception e) {
                LOG.error(
                        "Failed to refresh backing stream for method {}",
                        method.getFullMethodName(),
                        e);
                // return error to user
                respStream.onError(e);
            }
        }

        // this is just stored to cancel if the call fails before
        // being established
        private StreamObserver<ReqT> initialReqStream = null;
    }
    /** @param failedAttemptNumber number of the attempt which just failed, 1-based */
    static long delayAfterFailureMs(int failedAttemptNumber) {
        // backoff delay pattern: 0, [500ms - 1sec), 2sec, 4sec, 8sec, 8sec, ... (jitter after first
        // retry)
        if (failedAttemptNumber <= 1) {
            return 0L;
        }
        return failedAttemptNumber == 2
                ? 500L + ThreadLocalRandom.current().nextLong(500L)
                : (2000L << Math.min(failedAttemptNumber - 3, 2));
    }
}

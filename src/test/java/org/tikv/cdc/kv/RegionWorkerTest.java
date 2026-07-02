package org.tikv.cdc.kv;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.tikv.cdc.CDCConfig;
import org.tikv.cdc.model.OperateType;
import org.tikv.cdc.model.RawKVEntry;
import org.tikv.cdc.model.RegionFeedEvent;
import org.tikv.cdc.model.RegionKeyRange;
import org.tikv.cdc.model.RegionStatefulEvent;
import org.tikv.cdc.model.RegionVerId;
import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static org.tikv.cdc.model.RegionKeyRange.UPPER_BOUND_KEY;

public class RegionWorkerTest {

    BlockingQueue<RegionFeedEvent> eventsBuffer = new LinkedBlockingQueue<>(1024);
    private Consumer<RegionFeedEvent> eventConsumer =
            new Consumer<RegionFeedEvent>() {
                @Override
                public void accept(RegionFeedEvent regionFeedEvent) {
                    eventsBuffer.offer(regionFeedEvent);
                }
            };
    CDCConfig cdcConfig;

    @Before
    public void before() {
        cdcConfig = new CDCConfig();
    }

    @Test
    public void handleResolvedTs() {
        RegionWorker regionWorker = new RegionWorker(null, null, eventConsumer, null, cdcConfig);

        RegionStateManager.RegionFeedState s1 =
                new RegionStateManager.RegionFeedState(
                        new RegionStateManager.SingleRegionInfo(
                                new RegionVerId(1, 1, 1), null, null),
                        1);
        s1.setInitialized();
        s1.updateResolvedTs(9);

        RegionStateManager.RegionFeedState s2 =
                new RegionStateManager.RegionFeedState(
                        new RegionStateManager.SingleRegionInfo(
                                new RegionVerId(2, 2, 2), null, null),
                        1);
        s2.setInitialized();
        s2.updateResolvedTs(11);

        RegionStateManager.RegionFeedState s3 =
                new RegionStateManager.RegionFeedState(
                        new RegionStateManager.SingleRegionInfo(
                                new RegionVerId(3, 3, 3), null, null),
                        3);
        s3.updateResolvedTs(8);
        RegionStatefulEvent.ResolvedTsEvent rte = new RegionStatefulEvent.ResolvedTsEvent();
        rte.setResolvedTs(10);
        List<RegionStateManager.RegionFeedState> regions = new ArrayList<>();
        regions.add(s1);
        regions.add(s2);
        regions.add(s3);
        rte.setRegions(regions);
        regionWorker.handleResolvedTs(rte);
        Assert.assertEquals(10, s1.getLastResolvedTs());
        Assert.assertEquals(11, s2.getLastResolvedTs());
        Assert.assertEquals(8, s3.getLastResolvedTs());
        RegionFeedEvent event = eventsBuffer.poll();
        Assert.assertEquals(0, event.getRegionId());
        Assert.assertEquals(10, event.getResolved().getResolvedTs());
        Assert.assertEquals(1, event.getResolved().getKeyRanges().size());
        Assert.assertEquals(1, event.getResolved().getKeyRanges().get(0).getRegionId());
    }

    @Test
    @Ignore("Legacy test constructs an incomplete RPC context and malformed protobuf builders")
    public void handleEventEntryOutOfOrder() {
        RegionKeyRange.ComparableKeyRange range =
                new RegionKeyRange.ComparableKeyRange(new byte[] {}, UPPER_BOUND_KEY);
        RegionStateManager.RegionFeedState state =
                new RegionStateManager.RegionFeedState(
                        new RegionStateManager.SingleRegionInfo(
                                new RegionVerId(1, 1, 1), range, null),
                        0);
        state.start();
        RPCContext rpcContext =
                RPCContext.Builder.newBuilder().setAddress("127.0.0.1:2379").build();
        EventFeedStream stream = new EventFeedStream("127.0.0.1:2379", 1, rpcContext, 1000);
        RegionWorker worker = new RegionWorker(null, stream, eventConsumer, null, cdcConfig);
        Cdcpb.Event events =
                Cdcpb.Event.newBuilder()
                        .setEntries(
                                Cdcpb.Event.Entries.newBuilder()
                                        .addEntries(
                                                Cdcpb.Event.Row.newBuilder()
                                                        .setStartTs(1)
                                                        .setType(Cdcpb.Event.LogType.PREWRITE)
                                                        .setOpType(Cdcpb.Event.Row.OpType.PUT)
                                                        .setKey(
                                                                ByteString.copyFrom(
                                                                        new byte[] {'k', 'e', 'y'}))
                                                        .setOldValue(
                                                                ByteString.copyFrom(
                                                                        new byte[] {
                                                                            'o', 'l', 'd', 'v', 'a',
                                                                            'l', 'u', 'e'
                                                                        }))
                                                        .build())
                                        .build())
                        .build();
        worker.handleEventEntry(events.getEntries(), state);

        Cdcpb.Event.Row crow =
                Cdcpb.Event.Row.newBuilder()
                        .setStartTs(1)
                        .setCommitTs(2)
                        .setType(Cdcpb.Event.LogType.COMMIT)
                        .setOpType(Cdcpb.Event.Row.OpType.PUT)
                        .setKey(ByteString.copyFrom(new byte[] {'k', 'e', 'y'}))
                        .build();
        List<Cdcpb.Event.Row> entriesList =
                new ArrayList<>(events.getEntriesOrBuilder().getEntriesList());
        entriesList.add(crow);
        Cdcpb.Event.Entries entries = Cdcpb.Event.Entries.newBuilder().build();
        entriesList.forEach(
                entry -> {
                    entries.newBuilder().addEntries(entry);
                });

        //        worker.handleEventEntry(Cdcpb.Event.Entries.newBuilder().addEntries(entriesList),
        // state);
        Assert.assertEquals(0, eventsBuffer.size());

        events.getEntriesOrBuilder()
                .getEntriesList()
                .add(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setType(Cdcpb.Event.LogType.PREWRITE)
                                .setOpType(Cdcpb.Event.Row.OpType.PUT)
                                .setKey(ByteString.copyFrom(new byte[] {'k', 'e', 'y'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', 'a', 'l', 'u', 'e'}))
                                .setOldValue(
                                        ByteString.copyFrom(
                                                new byte[] {
                                                    'o', 'l', 'd', 'v', 'a', 'l', 'u', 'e'
                                                }))
                                .build());
        worker.handleEventEntry(events.getEntries(), state);
        Assert.assertEquals(0, eventsBuffer.size());

        events.getEntriesOrBuilder()
                .getEntriesList()
                .add(Cdcpb.Event.Row.newBuilder().setType(Cdcpb.Event.LogType.INITIALIZED).build());
        worker.handleEventEntry(events.getEntries(), state);
        RegionFeedEvent revent = eventsBuffer.poll();
        Assert.assertEquals(
                revent.getRawKVEntry(),
                new RawKVEntry.Builder()
                        .setOpType(OperateType.Insert)
                        .setKey(ByteString.copyFrom(new byte[] {'k', 'e', 'y'}))
                        .setValue(ByteString.copyFrom(new byte[] {'v', 'a', 'l', 'u', 'e'}))
                        .setOldValue(
                                ByteString.copyFrom(
                                        new byte[] {'o', 'l', 'd', 'v', 'a', 'l', 'u', 'e'}))
                        .setRegionId(0L)
                        .setStartTs(1L)
                        .setCrts(2L));
        Assert.assertEquals(1, eventsBuffer.size());
    }
}

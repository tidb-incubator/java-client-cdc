package org.tikv.cdc.kv;

import org.junit.Assert;
import org.junit.Test;
import org.tikv.cdc.model.MutableEventRow;
import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.List;

public class MatcherTest {
    private Matcher matcher;

    @Test
    public void matcherRowTest() {
        matcher = new Matcher();
        matcher.putPrewriteRow(
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '2'}))
                                .build()));
        matcher.putPrewriteRow(
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(2)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '2'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                                .build()));
        matcher.rollbackRow(
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .build()));

        Cdcpb.Event.Row commitRow1 =
                Cdcpb.Event.Row.newBuilder()
                        .setStartTs(1)
                        .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                        .build();
        boolean ok1 = matcher.matchRow(new MutableEventRow(commitRow1), true);
        Assert.assertFalse(ok1);
        MutableEventRow commitRow2 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(2)
                                .setCommitTs(3)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .build());

        boolean ok2 = matcher.matchRow(commitRow2, true);
        Assert.assertTrue(ok2);
        Assert.assertEquals(
                commitRow2.getEventRow(),
                Cdcpb.Event.Row.newBuilder()
                        .setStartTs(2)
                        .setCommitTs(3)
                        .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                        .setValue(ByteString.copyFrom(new byte[] {'v', '2'}))
                        .setOldValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                        .build());
    }

    @Test
    public void matchRowUninitialized() {
        matcher = new Matcher();
        matcher.putPrewriteRow(
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'v', '4'}))
                                .build()));
        MutableEventRow commitRow1 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setCommitTs(2)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .build());
        boolean ok1 = matcher.matchRow(commitRow1, false);
        Assert.assertFalse(ok1);
        Assert.assertEquals(
                commitRow1.getEventRow(),
                Cdcpb.Event.Row.newBuilder()
                        .setStartTs(1)
                        .setCommitTs(2)
                        .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                        .build());
        matcher.cacheCommitRow(commitRow1);

        matcher.putPrewriteRow(
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'v', '4'}))
                                .build()));
        matcher.putPrewriteRow(
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(2)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'v', '4'}))
                                .build()));

        MutableEventRow commitRow2 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(2)
                                .setCommitTs(3)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .build());
        boolean ok2 = matcher.matchRow(commitRow2, false);
        Assert.assertTrue(ok2);
        Assert.assertEquals(
                commitRow2.getEventRow(),
                Cdcpb.Event.Row.newBuilder()
                        .setStartTs(2)
                        .setCommitTs(3)
                        .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                        .setValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                        .setOldValue(ByteString.copyFrom(new byte[] {'v', '4'}))
                        .build());
        List<MutableEventRow> rows = matcher.matchCachedRow(true);
        Assert.assertEquals(rows.size(), 1);
        Assert.assertEquals(
                rows.get(0).getEventRow(),
                Cdcpb.Event.Row.newBuilder()
                        .setStartTs(1)
                        .setCommitTs(2)
                        .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                        .setValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                        .setOldValue(ByteString.copyFrom(new byte[] {'v', '4'}))
                        .build());
    }

    @Test
    public void matchCachedRow() {
        matcher = new Matcher();
        Assert.assertEquals(0, matcher.matchCachedRow(true).size());
        MutableEventRow commitRow1 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setCommitTs(2)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .build());
        matcher.cacheCommitRow(commitRow1);

        MutableEventRow commitRow2 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(3)
                                .setCommitTs(4)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .build());
        matcher.cacheCommitRow(commitRow2);

        MutableEventRow commitRow3 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(4)
                                .setCommitTs(5)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '3'}))
                                .build());
        matcher.cacheCommitRow(commitRow3);
        Assert.assertEquals(0, matcher.matchCachedRow(true).size());
        matcher.cacheCommitRow(commitRow1);
        matcher.cacheCommitRow(commitRow2);
        matcher.cacheCommitRow(commitRow3);

        MutableEventRow prewriteRow1 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '1'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '1'}))
                                .build());
        matcher.putPrewriteRow(prewriteRow1);

        MutableEventRow prewriteRow2 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(3)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '2'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '2'}))
                                .build());
        matcher.putPrewriteRow(prewriteRow2);
        MutableEventRow prewriteRow3 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(4)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '3'}))
                                .build());
        matcher.putPrewriteRow(prewriteRow3);

        List<MutableEventRow> mer = matcher.matchCachedRow(true);
        Assert.assertEquals(mer.size(), 2);

        MutableEventRow match1 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setCommitTs(2)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '1'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '1'}))
                                .build());
        Assert.assertEquals(mer.get(0).getEventRow(), match1.getEventRow());
        MutableEventRow match2 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(3)
                                .setCommitTs(4)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '2'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '2'}))
                                .build());
        Assert.assertEquals(mer.get(1).getEventRow(), match2.getEventRow());
    }

    @Test
    public void matchCachedRollbackRow() {
        matcher = new Matcher();
        matcher.matchCachedRollbackRow(true);
        MutableEventRow commitRow1 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .build());
        matcher.cacheRollbackRow(commitRow1);
        MutableEventRow commitRow2 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(3)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .build());
        matcher.cacheRollbackRow(commitRow2);

        MutableEventRow commitRow3 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(4)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '3'}))
                                .build());
        matcher.cacheRollbackRow(commitRow3);
        matcher.matchCachedRollbackRow(true);

        matcher.cacheRollbackRow(commitRow1);
        matcher.cacheRollbackRow(commitRow2);
        matcher.cacheRollbackRow(commitRow3);

        MutableEventRow prewriteRow1 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(1)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '1'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '1'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '1'}))
                                .build());
        matcher.putPrewriteRow(prewriteRow1);

        MutableEventRow prewriteRow2 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(3)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '2'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '2'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '2'}))
                                .build());
        matcher.putPrewriteRow(prewriteRow2);
        MutableEventRow prewriteRow3 =
                new MutableEventRow(
                        Cdcpb.Event.Row.newBuilder()
                                .setStartTs(4)
                                .setKey(ByteString.copyFrom(new byte[] {'k', '3'}))
                                .setValue(ByteString.copyFrom(new byte[] {'v', '3'}))
                                .setOldValue(ByteString.copyFrom(new byte[] {'o', 'v', '3'}))
                                .build());
        matcher.putPrewriteRow(prewriteRow3);
        matcher.matchCachedRollbackRow(true);
        Assert.assertTrue(matcher.getUnmatchedValue().isEmpty());
    }
}

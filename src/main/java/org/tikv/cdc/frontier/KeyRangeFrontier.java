package org.tikv.cdc.frontier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.model.RegionKeyRange;
import org.tikv.common.util.FastByteComparisons;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class KeyRangeFrontier implements Frontier {
    private static final Logger LOG = LoggerFactory.getLogger(KeyRangeFrontier.class);
    public static long FAKE_REGION_ID = 0L;
    private final SkipList spanList;
    private final FibonacciHeap minTsHeap;
    private final Map<Long, SkipList.SkipListNode> cachedRegions;
    //  private final Metrics metrics; // Assuming Metrics is a custom class to track metrics

    public KeyRangeFrontier(long checkpointTs, RegionKeyRange.ComparableKeyRange... spans) {
        this.spanList = new SkipList();
        this.minTsHeap = new FibonacciHeap();
        this.cachedRegions = new HashMap<>();

        // Initialize frontier with spans
        boolean firstRange = true;

        for (RegionKeyRange.ComparableKeyRange span : spans) {
            if (firstRange) {
                spanList.insert(span.getStart(), minTsHeap.insert(checkpointTs));
                spanList.insert(span.getEnd(), minTsHeap.insert(Long.MAX_VALUE));
                firstRange = false;
                continue;
            }
            insert(FAKE_REGION_ID, span, checkpointTs);
        }
    }

    public SkipList getSpanList() {
        return spanList;
    }

    public FibonacciHeap getMinTsHeap() {
        return minTsHeap;
    }

    @Override
    public long frontier() {
        return minTsHeap.getMinKey();
    }

    @Override
    public void forward(long regionID, RegionKeyRange.ComparableKeyRange keyRange, long ts) {
        SkipList.SkipListNode node = cachedRegions.get(regionID);
        if (node != null
                && node.regionID == regionID
                && keyRange.getEnd() != null
                && keyRange.getEnd().length != 0)
            if (FastByteComparisons.compareTo(node.getKey(), keyRange.getStart()) == 0
                    && FastByteComparisons.compareTo(node.getEnd(), keyRange.getEnd()) == 0) {
                // Update the timestamp for the region
                minTsHeap.updateKey(node.value, ts);
                return;
            }
        insert(regionID, keyRange, ts);
    }

    private void insert(long regionId, RegionKeyRange.ComparableKeyRange keyRange, long ts) {

        SkipList.SeekResult seekRes = spanList.seek(keyRange.getStart());

        SkipList.SkipListNode next = seekRes.node().next();
        if (next != null
                && next.key != null
                && next.key.length != 0
                && next.end != null
                && next.end.length != 0) {
            if (FastByteComparisons.compareTo(next.getKey(), keyRange.getStart()) == 0
                    && FastByteComparisons.compareTo(next.getKey(), keyRange.getEnd()) == 0) {
                minTsHeap.updateKey(seekRes.node().value, ts);
                cachedRegions.remove(seekRes.node().regionID);
                if (regionId != FAKE_REGION_ID) {
                    SkipList.SkipListNode newNode = seekRes.node();
                    newNode.regionID = regionId;
                    newNode.end = next.getKey();
                    cachedRegions.put(regionId, newNode);
                }
                return;
            }
        }
        SkipList.SkipListNode node = seekRes.node();
        cachedRegions.remove(node.regionID);
        long lastNodeTs = Long.MAX_VALUE;
        boolean shouldInsertStartNode = true;
        if (node.value != null) {
            lastNodeTs = node.value.getKey();
        }
        if (node.key == null) {
            node = node.next();
        }
        for (; node != null; node = node.next()) {
            cachedRegions.remove(node.regionID);

            int cmpStart = FastByteComparisons.compareTo(node.getKey(), keyRange.getStart());

            if (cmpStart < 0) {
                continue;
            }

            if (FastByteComparisons.compareTo(node.getKey(), keyRange.getEnd()) > 0) {
                break;
            }
            lastNodeTs = node.value.getKey();
            if (cmpStart == 0) {
                minTsHeap.updateKey(node.value, ts);
                shouldInsertStartNode = false;
            } else {
                spanList.remove(seekRes, node);
                minTsHeap.remove(node.value);
            }
        }
        if (shouldInsertStartNode) {
            spanList.insertNextToNode(seekRes, keyRange.getStart(), minTsHeap.insert(ts));
            seekRes.next(); // re seek.
        }
        spanList.insertNextToNode(seekRes, keyRange.getEnd(), minTsHeap.insert(lastNodeTs));
    }

    @Override
    public void entries(FrontierConsumer fn) {
        spanList.entries(
                (node) -> {
                    fn.accept(node.getKey(), node.value.getKey());
                    return true;
                });
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        this.entries(
                (key, ts) -> {
                    if (ts == Long.MAX_VALUE) {
                        buf.append(
                                String.format(
                                        "[%s @ Max] ", new String(key, StandardCharsets.UTF_8)));
                    } else {
                        buf.append(
                                String.format(
                                        "[%s @ %d] ", new String(key, StandardCharsets.UTF_8), ts));
                    }
                });
        return buf.toString();
    }
}

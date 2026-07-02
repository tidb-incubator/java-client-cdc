package org.tikv.cdc.model;

import org.tikv.kvproto.Cdcpb;

import java.util.List;

public class RegionFeedEvent {
    private long regionId;

    private RawKVEntry rawKVEntry = null;

    private ResolvedKeyRanges resolved = null;

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public RawKVEntry getRawKVEntry() {
        return rawKVEntry;
    }

    public void setRawKVEntry(RawKVEntry rawKVEntry) {
        this.rawKVEntry = rawKVEntry;
    }

    public ResolvedKeyRanges getResolved() {
        return resolved;
    }

    public void setResolved(ResolvedKeyRanges resolved) {
        this.resolved = resolved;
    }

    @Override
    public String toString() {
        return "RegionFeedEvent{"
                + "regionId="
                + regionId
                + ", rawKVEntry="
                + rawKVEntry
                + ", resolved="
                + resolved
                + '}';
    }

    public static class ResolvedKeyRanges {
        private List<RegionKeyRange> keyRanges;
        private long resolvedTs;

        public List<RegionKeyRange> getKeyRanges() {
            return keyRanges;
        }

        public void setKeyRanges(List<RegionKeyRange> keyRanges) {
            this.keyRanges = keyRanges;
        }

        public long getResolvedTs() {
            return resolvedTs;
        }

        public void setResolvedTs(long resolvedTs) {
            this.resolvedTs = resolvedTs;
        }

        @Override
        public String toString() {
            return "ResolvedKeyRanges{"
                    + "keyRanges="
                    + keyRanges
                    + ", resolvedTs="
                    + resolvedTs
                    + '}';
        }
    }

    public static RegionFeedEvent assembleRowEvent(long regionId, Cdcpb.Event.Row row) {
        RawKVEntry rawKVEntry =
                new RawKVEntry.Builder()
                        .setOpType(OperateType.fromType(row))
                        .setRegionId(regionId)
                        .setKey(row.getKey())
                        .setValue(row.getValue())
                        .setStartTs(row.getStartTs())
                        .setCrts(row.getCommitTs())
                        .setOldValue(row.getOldValue())
                        .build();
        RegionFeedEvent reEvent = new RegionFeedEvent();
        reEvent.setRegionId(regionId);
        reEvent.setRawKVEntry(rawKVEntry);
        // 优先队列对比   用resolvedTs
        RegionFeedEvent.ResolvedKeyRanges resolvedKeyRanges =
                new RegionFeedEvent.ResolvedKeyRanges();
        resolvedKeyRanges.setResolvedTs(row.getCommitTs());
        reEvent.setResolved(resolvedKeyRanges);
        return reEvent;
    }
}

package org.tikv.cdc;

import java.util.concurrent.atomic.AtomicLong;

public class TableStoreStat {
    private final AtomicLong regionCount;
    private final AtomicLong resolvedTs;
    private final AtomicLong commitTs;

    public TableStoreStat() {
        this.regionCount = new AtomicLong(0);
        this.resolvedTs = new AtomicLong(0);
        this.commitTs = new AtomicLong(0);
    }

    public long getRegionCount() {
        return regionCount.get();
    }

    public void setRegionCount(long regionCount) {
        this.regionCount.set(regionCount);
    }

    public void incrementRegionCount() {
        regionCount.incrementAndGet();
    }

    public void decrementRegionCount() {
        regionCount.decrementAndGet();
    }

    public long getResolvedTs() {
        return resolvedTs.get();
    }

    public void setResolvedTs(long resolvedTs) {
        this.resolvedTs.set(resolvedTs);
    }

    public long getCommitTs() {
        return commitTs.get();
    }

    public void setCommitTs(long commitTs) {
        this.commitTs.set(commitTs);
    }

    @Override
    public String toString() {
        return "TableStoreStat{"
                + "regionCount="
                + regionCount
                + ", resolvedTs="
                + resolvedTs
                + ", commitTs="
                + commitTs
                + '}';
    }
}

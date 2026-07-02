package org.tikv.cdc.model;

import org.tikv.common.meta.TiTableInfo;

/** Polymorphic event */
public class PolymorphicEvent {
    private long startTs;
    private long crTs;
    private RawKVEntry rawKVEntry;
    private String databaseName;
    private TiTableInfo tableInfo;

    public PolymorphicEvent(
            long startTs,
            long crTs,
            String databaseName,
            RawKVEntry rawKVEntry,
            TiTableInfo tableInfo) {
        this.startTs = startTs;
        this.crTs = crTs;
        this.rawKVEntry = rawKVEntry;
        this.databaseName = databaseName;
        this.tableInfo = tableInfo;
    }

    public PolymorphicEvent(final long ts) {
        this.crTs = ts;
    }

    public RawKVEntry getRawKVEntry() {
        return rawKVEntry;
    }

    public void setRawKVEntry(RawKVEntry rawKVEntry) {
        this.rawKVEntry = rawKVEntry;
    }

    public long getCrTs() {
        return crTs;
    }

    public void setCrTs(long crTs) {
        this.crTs = crTs;
    }

    public long getStartTs() {
        return startTs;
    }

    public void setStartTs(long startTs) {
        this.startTs = startTs;
    }

    public TiTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TiTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void release() {
        if (rawKVEntry != null) {
            // 确保 rawKVEntry 引用被清除， 让GC 回收整个对象
            this.rawKVEntry = null;
        }
    }

    @Override
    public String toString() {
        return "PolymorphicEvent{"
                + "startTs="
                + startTs
                + ", crTs="
                + crTs
                + ", rawKVEntry="
                + rawKVEntry
                + ", databaseName='"
                + databaseName
                + '\''
                + ", tableInfo="
                + tableInfo
                + '}';
    }
}

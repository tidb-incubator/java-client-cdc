package org.tikv.cdc.kv;

import org.tikv.cdc.model.RawKVEntry;

/** Listening to log interface */
public interface EntryListener {
    /** send */
    void notify(RawKVEntry entry);

    void resolvedTs(long resolvedTs);

    void close();
}

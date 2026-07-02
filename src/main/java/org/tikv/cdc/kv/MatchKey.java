package org.tikv.cdc.kv;

import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.Objects;

public class MatchKey {
    private long startTs;
    private ByteString key;

    public MatchKey(long startTs, ByteString key) {
        this.startTs = startTs;
        this.key = key;
    }

    public long getStartTs() {
        return startTs;
    }

    public ByteString getKey() {
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MatchKey matchKey = (MatchKey) obj;
        return startTs == matchKey.startTs && key.equals(matchKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTs, key);
    }
}

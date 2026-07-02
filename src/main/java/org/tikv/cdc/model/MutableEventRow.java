package org.tikv.cdc.model;

import org.tikv.kvproto.Cdcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

public class MutableEventRow {
    private Cdcpb.Event.Row eventRow;

    public MutableEventRow(Cdcpb.Event.Row eventRow) {
        this.eventRow = eventRow;
    }

    public void setKey(ByteString key) {
        this.eventRow = eventRow.toBuilder().setKey(key).build();
    }

    public ByteString getKey() {
        return this.eventRow.getKey();
    }

    public long getStartTs() {
        return this.eventRow.getStartTs();
    }

    public void setValue(ByteString value) {
        this.eventRow = eventRow.toBuilder().setValue(value).build();
    }

    public ByteString getValue() {
        return this.eventRow.getValue();
    }

    public void setOldValue(ByteString oldValue) {
        this.eventRow = eventRow.toBuilder().setOldValue(oldValue).build();
    }

    public ByteString getOldValue() {
        return this.eventRow.getOldValue();
    }

    public Cdcpb.Event.Row getEventRow() {
        return eventRow;
    }

    @Override
    public String toString() {
        return "MutableEventRow{" + "eventRow=" + eventRow + '}';
    }
}

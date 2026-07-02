package org.tikv.cdc.model;

import org.tikv.kvproto.Cdcpb;

public enum OperateType {
    Ddl(0),
    Insert(1),
    Delete(2),
    Update(3),
    Heatbeat(4),
    Resolved(6),
    Unknown(7);

    private int type;

    OperateType(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }

    public static OperateType fromType(Cdcpb.Event.Row row) {
        if (row.getOpType().equals(Cdcpb.Event.Row.OpType.PUT)) {
            if ((row.getOldValue() != null && !row.getOldValue().isEmpty())
                    && row.getValue() != null) {
                return OperateType.Update;
            } else {
                return OperateType.Insert;
            }
        }
        if (row.getOpType().equals(Cdcpb.Event.Row.OpType.DELETE)) {
            return OperateType.Delete;
        }
        return Unknown;
    }

    public static OperateType valueOf(final int type) {
        for (OperateType opType : OperateType.values()) {
            if (opType.getType() == type) {
                return opType;
            }
        }
        throw new IllegalArgumentException("No enum constant with type " + type);
    }
}

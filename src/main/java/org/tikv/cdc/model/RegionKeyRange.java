package org.tikv.cdc.model;

import org.apache.commons.codec.binary.Hex;
import org.tikv.common.apiversion.CodecUtils;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

public class RegionKeyRange {
    public static final long JOB_TABLE_ID =
            92; // Assuming JobTableID is 92 based on tidb/ddl package

    public static final byte[] UPPER_BOUND_KEY =
            new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

    private long regionId;
    private ComparableKeyRange keyRange;

    public RegionKeyRange(long regionId, ComparableKeyRange keyRange) {
        this.regionId = regionId;
        this.keyRange = keyRange;
    }

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public ComparableKeyRange getKeyRange() {
        return keyRange;
    }

    public void setKeyRange(ComparableKeyRange keyRange) {
        this.keyRange = keyRange;
    }

    public static class ComparableKeyRange {
        private byte[] start;
        private byte[] end;

        public ComparableKeyRange(byte[] start, byte[] end) {
            this.start = start;
            this.end = end;
        }

        public ComparableKeyRange hackKeyRange(byte[] originStart, byte[] originEnd) {
            byte[] start = originStart;
            byte[] end = originEnd;
            if (start == null || start.length == 0) {
                start = new byte[0];
            }
            if (end == null || end.length == 0) {
                end = UPPER_BOUND_KEY;
            }
            return new ComparableKeyRange(start, end);
        }

        public ComparableKeyRange hack() {
            return hackKeyRange(this.start, this.end);
        }

        public byte[] getStart() {
            return start;
        }

        public byte[] getEnd() {
            return end;
        }

        public void setStart(byte[] start) {
            this.start = start;
        }

        public void setEnd(byte[] end) {
            this.end = end;
        }

        @Override
        public String toString() {
            return "ComparableKeyRange{"
                    + "start="
                    + Hex.encodeHexString(start)
                    + ", end="
                    + Hex.encodeHexString(end)
                    + '}';
        }
    }

    public static boolean keyInRange(byte[] k, ComparableKeyRange span) {
        return startCompare(k, span.getStart()) >= 0 && endCompare(k, span.getEnd()) < 0;
    }

    public static boolean isSubKeyRange(ComparableKeyRange sub, ComparableKeyRange... parents) {
        if (FastByteComparisons.compareTo(sub.getStart(), sub.getEnd()) >= 0) {
            throw new IllegalArgumentException("The sub span is invalid: " + sub);
        }
        for (ComparableKeyRange parent : parents) {
            if (startCompare(parent.getStart(), sub.getStart()) <= 0
                    && endCompare(sub.getEnd(), parent.getEnd()) <= 0) {
                return true;
            }
        }
        return false;
    }

    public static ComparableKeyRange intersect(ComparableKeyRange lhs, ComparableKeyRange rhs)
            throws Exception {
        if ((lhs.getStart() != null && endCompare(lhs.getStart(), rhs.getEnd()) >= 0)
                || (rhs.getStart() != null && endCompare(rhs.getStart(), lhs.getEnd()) >= 0)) {
            throw new Exception("No overlap between key range: " + lhs + ", " + rhs);
        }

        byte[] start = lhs.getStart();
        if (startCompare(rhs.getStart(), start) > 0) {
            start = rhs.getStart();
        }

        byte[] end = lhs.getEnd();
        if (endCompare(rhs.getEnd(), end) < 0) {
            end = rhs.getEnd();
        }

        return new ComparableKeyRange(start, end);
    }

    public static int startCompare(byte[] lhs, byte[] rhs) {
        if (lhs == null && rhs == null) return 0;
        if (lhs == null) return -1;
        if (rhs == null) return 1;
        return FastByteComparisons.compareTo(lhs, 0, lhs.length, rhs, 0, rhs.length);
    }

    public static int endCompare(byte[] lhs, byte[] rhs) {
        if (lhs == null && rhs == null) return 0;
        if (lhs == null) return 1;
        if (rhs == null) return -1;
        return FastByteComparisons.compareTo(lhs, 0, lhs.length, rhs, 0, rhs.length);
    }

    public static ComparableKeyRange toComparableKeyRange(Coprocessor.KeyRange keyRange) {
        return new ComparableKeyRange(
                codecEncodeBytes(keyRange.getStart()), codecEncodeBytes(keyRange.getEnd()));
    }

    public static Coprocessor.KeyRange toKeyRange(ComparableKeyRange keyRange) {
        return Coprocessor.KeyRange.newBuilder()
                .setStart(CodecUtils.decode(ByteString.copyFrom(keyRange.getStart())))
                .setEnd(CodecUtils.decode(ByteString.copyFrom(keyRange.getEnd())))
                .build();
    }

    public static byte[] toComparableKey(ByteString key) {
        return codecEncodeBytes(key);
    }

    private static byte[] codecEncodeBytes(ByteString data) {
        return CodecUtils.encode(data).toByteArray();
    }
}

package org.tikv.common.util;

import org.joda.time.DateTimeZone;
import org.tikv.common.ExtendedDateTime;
import org.tikv.common.codec.Codec;
import org.tikv.common.handle.Handle;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.Converter;
import org.tikv.common.types.MySQLType;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.tikv.common.codec.TableCodec.decodeObjects;

public class TiDBKeyDecode {

    // from RowDecoderV2 decodeTimestamp
    private static Timestamp decodeTimestamp(long timestamp, DateTimeZone tz) {
        ExtendedDateTime extendedDateTime = Codec.DateTimeCodec.fromPackedLong(timestamp, tz);
        // Even though null is filtered out but data like 0000-00-00 exists
        // according to MySQL JDBC behavior, it can chose the **ROUND** behavior converted to the
        // nearest
        // value which is 0001-01-01.
        if (extendedDateTime == null) {
            return Codec.DateTimeCodec.createExtendedDateTime(tz, 1, 1, 1, 0, 0, 0, 0)
                    .toTimeStamp();
        }
        return extendedDateTime.toTimeStamp();
    }

    public static Object[] decodeObjectsV2(byte[] byteArray, Handle handle, TiTableInfo tableInfo) {
        Object[] tiKVValue = decodeObjects(byteArray, handle, tableInfo);

        List<TiColumnInfo> pkColumns =
                tableInfo.getColumns().stream()
                        .filter(TiColumnInfo::isPrimaryKey)
                        .sorted(Comparator.comparingInt(TiColumnInfo::getOffset))
                        .collect(Collectors.toList());

        // TiIndexInfo pk = tableInfo.getPrimaryKey(); 聚簇索引返回null，当前不能使用这个
        for (TiColumnInfo pkColumn : pkColumns) {
            MySQLType type = pkColumn.getType().getType();
            int offset = pkColumn.getOffset();
            Object value = tiKVValue[offset];
            switch (type) {
                case TypeString:
                case TypeVarString:
                case TypeVarchar:
                    if (tiKVValue[offset] instanceof byte[]) {
                        tiKVValue[offset] =
                                new String((byte[]) value, StandardCharsets.UTF_8)
                                        .replace("\u0000", "");
                    }
                case TypeDate:
                    if (tiKVValue[offset] instanceof Long) {
                        tiKVValue[offset] =
                                new Date(
                                        decodeTimestamp((long) value, Converter.getLocalTimezone())
                                                .getTime());
                    }
                case TypeDatetime:
                    if (tiKVValue[offset] instanceof Long) {
                        tiKVValue[offset] =
                                decodeTimestamp((long) value, Converter.getLocalTimezone());
                    }
                case TypeTimestamp:
                    if (tiKVValue[offset] instanceof Long) {
                        tiKVValue[offset] = decodeTimestamp((long) value, DateTimeZone.UTC);
                    }
            }
        }
        return tiKVValue;
    }
}

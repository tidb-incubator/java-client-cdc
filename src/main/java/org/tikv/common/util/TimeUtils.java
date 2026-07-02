package org.tikv.common.util;

import java.time.Duration;
import java.time.Instant;

public class TimeUtils {
    public static final Duration MIN_DURATION = Duration.ofSeconds(Long.MIN_VALUE, 0);
    public static final Duration MAX_DURATION = Duration.ofSeconds(Long.MAX_VALUE, 0);

    public static Duration timeSince(Instant eventTime) {
        Instant now = Instant.now();
        return Duration.between(eventTime, now);
    }
    /**
     * 实现类似 Go 的 Time.Sub 方法
     *
     * @param t 结束时间
     * @param u 开始时间
     * @return t - u 的时间差，溢出时返回最大/最小值
     */
    public static Duration sub(Instant t, Instant u) {
        // 1. 计算秒和纳秒差值
        long secondsDiff;
        int nanosDiff;
        try {
            secondsDiff = Math.subtractExact(t.getEpochSecond(), u.getEpochSecond());
            nanosDiff = Math.subtractExact(t.getNano(), u.getNano());
        } catch (ArithmeticException e) {
            return t.isAfter(u) ? MAX_DURATION : MIN_DURATION;
        }

        // 2. 处理纳秒借位
        if (nanosDiff < 0) {
            try {
                secondsDiff = Math.subtractExact(secondsDiff, 1);
                nanosDiff += 1_000_000_000;
            } catch (ArithmeticException e) {
                return t.isAfter(u) ? MAX_DURATION : MIN_DURATION;
            }
        }

        // 3. 构造 Duration
        Duration d;
        try {
            d = Duration.ofSeconds(secondsDiff, nanosDiff);
        } catch (ArithmeticException e) {
            return t.isAfter(u) ? MAX_DURATION : MIN_DURATION;
        }

        // 4. 验证计算结果
        try {
            Instant uPlusD = u.plus(d);
            if (uPlusD.equals(t)) {
                return d;
            }
        } catch (ArithmeticException e) {
            // 时间加法溢出
        }

        // 5. 处理溢出情况
        return t.isBefore(u) ? MIN_DURATION : MAX_DURATION;
    }
}

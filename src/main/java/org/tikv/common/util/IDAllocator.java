package org.tikv.common.util;

import java.util.concurrent.atomic.AtomicLong;

public class IDAllocator {
    // 定义一个 AtomicLong 用于存储当前的 request ID
    private static final AtomicLong currentRequestID = new AtomicLong(0);
    private static final AtomicLong streamClientID = new AtomicLong(0);
    // allocateRequestID 方法，生成并返回一个唯一的 request ID
    public static long allocateRequestID() {
        return currentRequestID.incrementAndGet();
    }

    public static long allocateStreamClientID() {
        return streamClientID.incrementAndGet();
    }
}

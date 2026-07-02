package org.tikv.cdc;

public class CDCConfig {
    private static final int EVENT_BUFFER_SIZE = 2000000;
    private static final int MAX_ROW_KEY_SIZE = 12 * 1024 * 1024;
    private static final int DEFAULT_WORKER_POOL_SIZE = 4;
    private static final double DEFAULT_EVENT_RATE_LIMIT = 20000;

    private int eventBufferSize = EVENT_BUFFER_SIZE;
    private int maxRowKeySize = MAX_ROW_KEY_SIZE;
    private int workerPoolSize = DEFAULT_WORKER_POOL_SIZE;
    private double eventRateLimit = DEFAULT_EVENT_RATE_LIMIT;

    public void setEventBufferSize(final int bufferSize) {
        eventBufferSize = bufferSize;
    }

    public void setMaxRowKeySize(final int rowKeySize) {
        maxRowKeySize = rowKeySize;
    }

    public int getEventBufferSize() {
        return eventBufferSize;
    }

    public double getEventRateLimit() {
        return eventRateLimit;
    }

    public int getMaxRowKeySize() {
        return maxRowKeySize;
    }

    public int getWorkerPoolSize() {
        return workerPoolSize;
    }

    public void setWorkerPoolSize(int workerPoolSize) {
        this.workerPoolSize = workerPoolSize;
    }

    public void setEventRateLimit(double eventRateLimit) {
        this.eventRateLimit = eventRateLimit;
    }
}

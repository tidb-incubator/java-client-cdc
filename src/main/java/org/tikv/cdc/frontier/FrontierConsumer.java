package org.tikv.cdc.frontier;

@FunctionalInterface
public interface FrontierConsumer {
    void accept(byte[] key, long ts);
}

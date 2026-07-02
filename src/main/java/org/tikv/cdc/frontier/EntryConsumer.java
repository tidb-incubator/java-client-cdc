package org.tikv.cdc.frontier;

@FunctionalInterface
public interface EntryConsumer {
    boolean accept(SkipList.SkipListNode node);
}

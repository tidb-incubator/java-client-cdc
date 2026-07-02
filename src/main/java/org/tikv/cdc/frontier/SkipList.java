package org.tikv.cdc.frontier;

import org.tikv.common.util.FastByteComparisons;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;

public class SkipList {
    private static final int MAX_HEIGHT = 12;

    static class SkipListNode {
        byte[] key;
        FibonacciHeap.FibonacciHeapNode value;
        byte[] end;
        long regionID;
        SkipListNode[] nexts;

        public SkipListNode(byte[] key, FibonacciHeap.FibonacciHeapNode value, int height) {
            this.key = key;
            this.value = value;
            this.nexts = new SkipListNode[height];
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getEnd() {
            return end;
        }

        public SkipListNode next() {
            return nexts[0];
        }

        public long getRegionID() {
            return regionID;
        }
    }

    static class SeekResult {
        SkipListNode[] nodes;

        public SeekResult(int height) {
            this.nodes = new SkipListNode[height];
        }

        public SkipListNode node() {
            return nodes[0];
        }

        public void next() {
            SkipListNode nextNode = node().nexts[0];
            for (int i = 0; i < nextNode.nexts.length; i++) {
                nodes[i] = nextNode;
            }
        }
    }

    private final SkipListNode head = new SkipListNode(null, null, MAX_HEIGHT);
    private int height = 0;

    private final Random random = new SecureRandom();

    public SkipList() {
        head.nexts = new SkipListNode[MAX_HEIGHT];
    }

    public SkipListNode getHead() {
        return head;
    }

    public int getHeight() {
        return height;
    }

    // 获取随机高度
    private int randomHeight() {
        int h = 1;
        while (h < MAX_HEIGHT && random.nextInt() % 4 == 0) {
            h++;
        }
        return h;
    }

    // 查找 key 所在位置，返回每层的前驱节点
    public SeekResult seek(byte[] key) {
        SeekResult result = new SeekResult(MAX_HEIGHT);
        SkipListNode current = head;

        for (int level = height - 1; level >= 0; level--) {
            while (true) {
                SkipListNode next = current.nexts[level];
                if (next == null) {
                    result.nodes[level] = current;
                    break;
                }
                int cmp = FastByteComparisons.compareTo(key, next.key);
                if (cmp < 0) {
                    result.nodes[level] = current;
                    break;
                } else if (cmp == 0) {
                    for (; level >= 0; level--) {
                        result.nodes[level] = next;
                    }
                    return result;
                } else {
                    current = next;
                }
            }
        }

        return result;
    }

    // 插入新节点到 seek 结果后
    public void insertNextToNode(
            SeekResult seekR, byte[] key, FibonacciHeap.FibonacciHeapNode value) {
        SkipListNode current = seekR.node();
        if (current != null
                && current.key != null
                && current.key.length != 0
                && key != null
                && key.length != 0
                && !nextTo(current, key)) {
            throw new IllegalArgumentException(
                    "the InsertNextToNode function can only append node to the seek result.");
        }

        int h = randomHeight();
        if (h > height) {
            height = h;
        }

        SkipListNode newNode = new SkipListNode(key, value, h);

        for (int level = 0; level < h; level++) {
            SkipListNode prev = seekR.nodes[level];
            if (prev == null) {
                prev = head;
            }
            newNode.nexts[level] = prev.nexts[level];
            prev.nexts[level] = newNode;
        }
    }

    // 插入接口
    public void insert(byte[] key, FibonacciHeap.FibonacciHeapNode value) {
        SeekResult seekR = seek(key);
        insertNextToNode(seekR, key, value);
    }

    // 删除指定节点
    public void remove(SeekResult seekR, SkipListNode toRemove) {
        SkipListNode current = seekR.node();
        if (current == null || current.next() != toRemove) {
            throw new IllegalArgumentException(
                    "the Remove function can only remove node right next to the seek result.");
        }

        for (int level = 0; level < toRemove.nexts.length; level++) {
            seekR.nodes[level].nexts[level] = toRemove.nexts[level];
        }
    }

    // 返回第一个节点
    public SkipListNode first() {
        return head.next();
    }

    // 遍历所有节点
    public void entries(Function<SkipListNode, Boolean> fn) {
        SkipListNode node = first();
        while (node != null) {
            if (!fn.apply(node)) return;
            node = node.next();
        }
    }

    // 判断 key 是否紧接在 node 后面
    private boolean nextTo(SkipListNode node, byte[] key) {
        int cmp = FastByteComparisons.compareTo(node.key, key);
        if (cmp == 0) return true;
        if (cmp > 0) return false;

        SkipListNode next = node.next();
        if (next == null) return true;

        return FastByteComparisons.compareTo(next.key, key) > 0;
    }

    // 用于遍历的函数式接口
    @FunctionalInterface
    public interface Function<T, R> {
        R apply(T t);
    }

    // 打印整个列表
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        entries(
                node -> {
                    sb.append('[')
                            .append(new String(node.key, StandardCharsets.UTF_8))
                            .append("] ");
                    return true;
                });
        return sb.toString();
    }
}

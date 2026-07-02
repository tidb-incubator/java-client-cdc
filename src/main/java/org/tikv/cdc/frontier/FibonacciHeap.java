package org.tikv.cdc.frontier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FibonacciHeap {
    private static final Logger LOG = LoggerFactory.getLogger(FibonacciHeap.class);
    private FibonacciHeapNode min; // The node with the minimum key
    private FibonacciHeapNode root; // The root node (circular doubly linked list)
    private boolean dirty; // Flag indicating if heap is dirty (needs consolidation)

    public FibonacciHeap() {
        this.min = null;
        this.root = null;
        this.dirty = false;
    }

    // Get the minimum key in the heap
    public long getMinKey() {
        if (dirty) {
            consolidate();
            dirty = false;
        }
        return min.key;
    }

    // Insert a new node into the heap and return this new node
    public FibonacciHeapNode insert(long key) {
        FibonacciHeapNode x = new FibonacciHeapNode(key);
        addToRoot(x);
        if (min == null || min.key > x.key) {
            min = x;
        }
        return x;
    }

    // Remove a node from the heap
    public void remove(FibonacciHeapNode x) {
        if (x == min) {
            dirty = true;
        }

        FibonacciHeapNode child = x.children;
        boolean isLast = child == null;

        while (!isLast) {
            FibonacciHeapNode next = child.right;
            isLast = next == x.children;
            removeChildren(x, child);
            addToRoot(child);
            child = next;
        }

        FibonacciHeapNode parent = x.parent;
        if (parent != null) {
            removeChildren(parent, x);
            if (parent.marked) {
                cascadingCut(parent);
            } else {
                parent.marked = true;
            }
        } else {
            cutFromRoot(x);
        }
    }

    // Update the key of a node
    public void updateKey(FibonacciHeapNode x, long key) {
        if (x.key == key) return;

        if (x.key > key) {
            decreaseKey(x, key);
            LOG.trace("decrease key from x {} to {},now {}", x.key, key, min.key);
        } else {
            increaseKey(x, key);
            LOG.trace("increase key from x {} to {},now {}", x.key, key, min.key);
        }
    }

    public void entries(HeapConsumer fn) {
        heapNodeIterator(this.root, fn);
    }

    private void heapNodeIterator(FibonacciHeapNode node, HeapConsumer fn) {
        boolean firstStep = true;
        for (FibonacciHeapNode next = node;
                next != null && (next != node || firstStep);
                next = next.right) {
            firstStep = false;
            if (!fn.accept(next)) return;
            if (next.children != null) {
                heapNodeIterator(next.children, fn);
            }
        }
    }

    @FunctionalInterface
    public interface HeapConsumer {
        boolean accept(FibonacciHeapNode node);
    }

    private void increaseKey(FibonacciHeapNode x, long key) {
        if (x == min) dirty = true;
        x.key = key;

        FibonacciHeapNode child = x.children;
        boolean cascadingCut = false;
        boolean isLast = child == null;

        while (!isLast) {
            FibonacciHeapNode next = child.right;
            isLast = next == x.children;

            if (child.key < x.key) {
                removeChildren(x, child);
                addToRoot(child);
                if (x.marked) cascadingCut = true;
                x.marked = true;
            }

            child = next;
        }

        if (cascadingCut) {
            cascadingCut(x);
        }
    }

    private void decreaseKey(FibonacciHeapNode x, long key) {
        x.key = key;
        FibonacciHeapNode parent = x.parent;

        if (parent != null && parent.key > x.key) {
            removeChildren(parent, x);
            addToRoot(x);
            if (parent.isMarked()) {
                cascadingCut(parent);
            } else {
                parent.marked = true;
            }
        }

        if (x.parent == null && min.key > key) {
            min = x;
        }
    }

    private void cascadingCut(FibonacciHeapNode x) {
        x.marked = false;
        while (x.parent != null) {
            FibonacciHeapNode parent = x.parent;
            removeChildren(parent, x);
            addToRoot(x);
            x.marked = false;
            if (!parent.isMarked()) {
                parent.marked = true;
                break;
            }
            x = parent;
        }
    }

    private static final int CONSOLIDATE_TABLE_SIZE = 47;

    // Consolidate the heap to merge trees of equal rank
    private void consolidate() {
        FibonacciHeapNode[] table = new FibonacciHeapNode[CONSOLIDATE_TABLE_SIZE];
        FibonacciHeapNode x = root;
        int maxOrder = 0;
        min = root;

        do {
            FibonacciHeapNode y = x;
            x = x.right;
            FibonacciHeapNode z = table[(int) y.rank];
            while (z != null) {
                table[(int) y.rank] = null;
                if (y.key > z.key) {
                    FibonacciHeapNode temp = y;
                    y = z;
                    z = temp;
                }
                addChildren(y, z);
                z = table[(int) y.rank];
            }
            table[(int) y.rank] = y;
            if (y.rank > maxOrder) {
                maxOrder = (int) y.rank;
            }
        } while (x != root);

        root = null;
        for (FibonacciHeapNode node : table) {
            if (node != null) {
                if (min.key > node.key) {
                    min = node;
                }
                addToRoot(node);
            }
        }
    }

    private void addChildren(FibonacciHeapNode root, FibonacciHeapNode x) {
        root.children = insertInto(root.children, x);
        root.rank++;
        x.parent = root;
    }

    private void removeChildren(FibonacciHeapNode root, FibonacciHeapNode x) {
        root.children = cutFrom(root.children, x);
        root.rank--;
        x.parent = null;
    }

    private void addToRoot(FibonacciHeapNode x) {
        root = insertInto(root, x);
    }

    private void cutFromRoot(FibonacciHeapNode x) {
        root = cutFrom(root, x);
    }

    private FibonacciHeapNode insertInto(FibonacciHeapNode head, FibonacciHeapNode x) {
        if (head == null) {
            x.left = x;
            x.right = x;
        } else {
            head.left.right = x;
            x.right = head;
            x.left = head.left;
            head.left = x;
        }
        return x;
    }

    private FibonacciHeapNode cutFrom(FibonacciHeapNode head, FibonacciHeapNode x) {
        if (x.right == x) {
            x.right = null;
            x.left = null;
            return null;
        }

        x.right.left = x.left;
        x.left.right = x.right;
        FibonacciHeapNode ret = x.right;
        x.right = null;
        x.left = null;

        if (head == x) {
            return ret;
        }
        return head;
    }

    // FibonacciHeapNode represents a node in the Fibonacci heap
    public static class FibonacciHeapNode {
        private long key; // The key of the node
        private FibonacciHeapNode left, right; // Doubly linked list pointers
        private FibonacciHeapNode children; // A list of children nodes
        private FibonacciHeapNode parent; // The parent node
        private long rank; // The rank of the node (number of children)
        private boolean marked; // Whether the node is marked for cascading cut

        public FibonacciHeapNode(long key) {
            this.key = key;
            this.rank = 0;
            this.marked = false;
            this.left = this;
            this.right = this;
            this.children = null;
            this.parent = null;
        }

        public long getKey() {
            return key;
        }

        public FibonacciHeapNode getLeft() {
            return left;
        }

        public FibonacciHeapNode getRight() {
            return right;
        }

        public FibonacciHeapNode getChildren() {
            return children;
        }

        public FibonacciHeapNode getParent() {
            return parent;
        }

        public long getRank() {
            return rank;
        }

        public boolean isMarked() {
            return marked;
        }

        @Override
        public String toString() {
            return "FibonacciHeapNode{"
                    + "key="
                    + key
                    + ", left="
                    + left
                    + ", right="
                    + right
                    + ", rank="
                    + rank
                    + ", marked="
                    + marked
                    + '}';
        }
    }
}

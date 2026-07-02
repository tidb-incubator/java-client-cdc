package org.tikv.cdc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableStoreStats {
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Map<String, TableStoreStat> v;

    public TableStoreStats() {
        this.v = new ConcurrentHashMap<>();
    }

    public void put(String key, TableStoreStat stat) {
        rwLock.writeLock().lock();
        try {
            v.put(key, stat);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public TableStoreStat get(String key) {
        rwLock.readLock().lock();
        try {
            return v.get(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void remove(String key) {
        rwLock.writeLock().lock();
        try {
            v.remove(key);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean containsKey(String key) {
        rwLock.readLock().lock();
        try {
            return v.containsKey(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int size() {
        rwLock.readLock().lock();
        try {
            return v.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void lock() {
        rwLock.readLock().lock();
        rwLock.writeLock().lock();
    }

    public void unlock() {
        rwLock.readLock().unlock();
        rwLock.writeLock().unlock();
    }

    @Override
    public String toString() {
        rwLock.readLock().lock();
        try {
            return "TableStoreStats{" + "v=" + v + '}';
        } finally {
            rwLock.readLock().unlock();
        }
    }
}

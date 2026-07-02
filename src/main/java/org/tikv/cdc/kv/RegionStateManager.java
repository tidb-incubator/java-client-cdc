package org.tikv.cdc.kv;

import org.tikv.cdc.model.RegionKeyRange;
import org.tikv.cdc.model.RegionVerId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

public class RegionStateManager {

    private static final int MIN_REGION_STATE_BUCKET = 4;
    private static final int MAX_REGION_STATE_BUCKET = 16;

    public static final int STATE_NORMAL = 0;
    public static final int STATE_STOPPED = 1;
    public static final int STATE_REMOVED = 2;
    public List<SyncRegionFeedStateMap> states;
    public int bucket = 4;

    public RegionStateManager(int bucket) {
        if (bucket <= 0) {
            this.bucket = Runtime.getRuntime().availableProcessors();
            if (this.bucket > MAX_REGION_STATE_BUCKET) {
                this.bucket = MAX_REGION_STATE_BUCKET;
            }
            if (this.bucket < MIN_REGION_STATE_BUCKET) {
                this.bucket = MIN_REGION_STATE_BUCKET;
            }
        }
        states = new ArrayList<>(this.bucket);

        for (int i = 0; i < this.bucket; i++) {
            states.add(new SyncRegionFeedStateMap());
        }
    }

    public List<SyncRegionFeedStateMap> getStates() {
        return this.states;
    }

    public void setState(long regionId, RegionFeedState state) {
        int rbucket = getBucket(regionId);
        states.get(rbucket).setByRegion(regionId, state);
    }

    public RegionFeedState getState(long regionId) {
        int rbucket = getBucket(regionId);
        return states.get(rbucket).getByRegion(regionId);
    }

    public void delState(long regionId) {
        int rbucket = getBucket(regionId);
        states.get(rbucket).delByRegionID(regionId);
    }

    public int getBucket(long regionId) {
        return (int) (regionId % bucket);
    }

    public static class SingleRegionInfo {
        private final RegionVerId verID;
        private final RegionKeyRange.ComparableKeyRange keyRange;
        private final RPCContext rpcCtx;
        private final LockedRange lockedRange;

        public SingleRegionInfo(
                RegionVerId verID, RegionKeyRange.ComparableKeyRange keyRange, RPCContext rpcCtx) {
            this.verID = verID;
            this.keyRange = keyRange;
            this.rpcCtx = rpcCtx;
            this.lockedRange = new LockedRange();
        }

        public long getResolvedTs() {
            return lockedRange.getCheckpointTs();
        }

        public void setResolvedTs(long checkpointTs) {
            lockedRange.updateCheckpointTs(checkpointTs);
        }

        public RPCContext getRpcCtx() {
            return this.rpcCtx;
        }

        public RegionKeyRange.ComparableKeyRange getSpan() {
            return keyRange;
        }

        public RegionVerId getVerID() {
            return verID;
        }

        @Override
        public String toString() {
            return "SingleRegionInfo{" + "verID=" + verID + ", keyRange=" + keyRange + '}';
        }
    }

    public static class RegionFeedState {
        private final SingleRegionInfo sri;
        private final long requestID;
        private Matcher matcher;
        private final State state = new State();

        public RegionFeedState(SingleRegionInfo sri, long requestID) {
            this.sri = sri;
            this.requestID = requestID;
        }

        public void start() {
            this.matcher = new Matcher();
        }

        public void markStopped() {
            state.setStateStopped();
        }

        public Matcher getMatcher() {
            return this.matcher;
        }

        public RegionKeyRange.ComparableKeyRange getKeyRange() {
            return this.sri.keyRange;
        }

        public long getRegionId() {
            return this.sri.verID.getId();
        }

        public boolean isStale() {
            return state.isStopped() || state.isRemoved();
        }

        public boolean isInitialized() {
            return sri.lockedRange.isInitialized();
        }

        public void setInitialized() {
            sri.lockedRange.setInitialized(true);
        }

        public RegionVerId getRegionID() {
            return sri.verID;
        }

        public long getLastResolvedTs() {
            return sri.lockedRange.getCheckpointTs();
        }

        public SingleRegionInfo getSri() {
            return sri;
        }

        public void updateResolvedTs(long resolvedTs) {
            sri.lockedRange.updateCheckpointTs(resolvedTs);
        }

        public long getRequestID() {
            return requestID;
        }

        @Override
        public String toString() {
            return "RegionFeedState{" + "sri=" + sri + ", requestID=" + requestID + '}';
        }
    }

    static class State {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private volatile int v = STATE_NORMAL;

        public void setStateStopped() {
            lock.writeLock().lock();
            try {
                if (v == STATE_NORMAL) {
                    v = STATE_STOPPED;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public boolean isStopped() {
            lock.readLock().lock();
            try {
                return v == STATE_STOPPED;
            } finally {
                lock.readLock().unlock();
            }
        }

        public boolean isRemoved() {
            lock.readLock().lock();
            try {
                return v == STATE_REMOVED;
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    public static class SyncRegionFeedStateMap {
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final ConcurrentHashMap<Long, RegionFeedState> statesInternal =
                new ConcurrentHashMap<>();

        public void setByRequestID(long requestID, RegionFeedState state) {
            lock.writeLock().lock();
            try {
                statesInternal.put(requestID, state);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void iter(BiFunction<Long, RegionFeedState, Boolean> fn) {
            lock.readLock().lock();
            try {
                for (Map.Entry<Long, RegionFeedState> entry : statesInternal.entrySet()) {
                    if (!fn.apply(entry.getKey(), entry.getValue())) {
                        break;
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        public RegionFeedState takeByRequestID(long requestID) {
            lock.writeLock().lock();

            try {
                RegionFeedState state = statesInternal.get(requestID);
                if (state != null) {
                    statesInternal.remove(requestID);
                }
                return state;
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void delByRegionID(long regionID) {
            lock.writeLock().lock();
            try {
                statesInternal.remove(regionID);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void setByRegion(long regionId, RegionFeedState state) {
            lock.writeLock().lock();
            try {
                statesInternal.put(regionId, state);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public RegionFeedState getByRegion(long regionId) {
            lock.readLock().lock();
            try {
                return statesInternal.get(regionId);
            } finally {
                lock.readLock().unlock();
            }
        }

        public int size() {
            lock.readLock().lock();
            try {
                return statesInternal.size();
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public String toString() {
            return "SyncRegionFeedStateMap{" + "statesInternal=" + statesInternal + '}';
        }
    }

    // 简化的辅助类与结构体
    static class LockedRange {
        private final AtomicLong checkpointTs = new AtomicLong(0);
        private final AtomicLong initialized = new AtomicLong(0);

        public long getCheckpointTs() {
            return checkpointTs.get();
        }

        public void updateCheckpointTs(long ts) {
            checkpointTs.updateAndGet(prev -> Math.max(prev, ts));
        }

        public boolean isInitialized() {
            return initialized.get() != 0;
        }

        public void setInitialized(boolean value) {
            initialized.set(value ? 1 : 0);
        }
    }

    @Override
    public String toString() {
        return "RegionStateManager{" + "states=" + states + ", bucket=" + bucket + '}';
    }
}

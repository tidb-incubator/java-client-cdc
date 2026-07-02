package org.tikv.cdc.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class RegionTsManager {
    // 使用 HashMap 存储区域信息（非线程安全）
    private final Map<Long, RegionTsInfo> regionMap = new HashMap<>();

    // 使用 PriorityQueue 实现最小堆（按 resolvedTs 排序）
    private final PriorityQueue<RegionTsInfo> regionHeap =
            new PriorityQueue<>(Comparator.comparingLong(info -> info.getTs().getResolvedTs()));

    /** 对应 Go 的 Upsert 方法 */
    public void upsert(long regionId, long resolvedTs, Instant eventTime) {
        RegionTsInfo existing = regionMap.get(regionId);
        if (existing != null) {
            TsItem existingTs = existing.getTs();
            // 处理时间回退逻辑
            if (resolvedTs <= existingTs.getResolvedTs()
                    && eventTime.isAfter(existingTs.getEventTime())) {
                existingTs.setPenalty(existingTs.getPenalty() + 1);
                existingTs.setEventTime(eventTime);
            } else if (resolvedTs > existingTs.getResolvedTs()) {
                existingTs.setResolvedTs(resolvedTs);
                existingTs.setEventTime(eventTime);
                existingTs.setPenalty(0);
                // 强制重建堆以更新顺序（PriorityQueue 无直接更新方法）
                rebuildHeap();
            }
        } else {
            TsItem newTs = new TsItem(resolvedTs, eventTime, 0);
            RegionTsInfo newInfo = new RegionTsInfo(regionId, newTs);
            regionMap.put(regionId, newInfo);
            regionHeap.add(newInfo);
        }
    }

    public void insert(RegionTsManager.RegionTsInfo item) {
        this.regionMap.put(item.getRegionId(), item);
    }

    /** 强制重建堆（效率较低，但可确保顺序正确） */
    private void rebuildHeap() {
        List<RegionTsInfo> temp = new ArrayList<>(regionHeap);
        regionHeap.clear();
        regionHeap.addAll(temp);
    }

    /** Pop 方法 */
    public RegionTsInfo pop() {
        if (regionHeap.isEmpty()) return null;
        RegionTsInfo removed = regionHeap.poll();
        regionMap.remove(removed.getRegionId());
        return removed;
    }

    /** Remove 方法 */
    public RegionTsInfo remove(long regionId) {
        RegionTsInfo removed = regionMap.remove(regionId);
        if (removed != null) {
            regionHeap.remove(removed); // O(n) 操作
            rebuildHeap(); // 需要重新整理堆结构
        }
        return removed;
    }

    /** Len 方法 */
    public int size() {
        return regionMap.size();
    }

    /** 获取最小 resolvedTs 的区域（查看堆顶） */
    public RegionTsInfo peekEarliest() {
        return regionHeap.peek();
    }

    public static class TsItem {
        private long resolvedTs;
        private Instant eventTime;
        private int penalty;

        public TsItem(long resolvedTs, Instant eventTime, int penalty) {
            this.resolvedTs = resolvedTs;
            this.eventTime = eventTime;
            this.penalty = penalty;
        }

        // Getters and Setters
        public long getResolvedTs() {
            return resolvedTs;
        }

        public void setResolvedTs(long resolvedTs) {
            this.resolvedTs = resolvedTs;
        }

        public Instant getEventTime() {
            return eventTime;
        }

        public void setEventTime(Instant eventTime) {
            this.eventTime = eventTime;
        }

        public int getPenalty() {
            return penalty;
        }

        public void setPenalty(int penalty) {
            this.penalty = penalty;
        }
    }

    public static class RegionTsInfo {
        private final long regionId;
        private TsItem ts;

        public RegionTsInfo(long regionId, TsItem ts) {
            this.regionId = regionId;
            this.ts = ts;
        }

        public long getRegionId() {
            return regionId;
        }

        public TsItem getTs() {
            return ts;
        }

        public void setTs(TsItem ts) {
            this.ts = ts;
        }
    }
}

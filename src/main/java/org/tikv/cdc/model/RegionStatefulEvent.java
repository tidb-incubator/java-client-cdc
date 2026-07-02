package org.tikv.cdc.model;

import org.tikv.cdc.kv.RegionStateManager;
import org.tikv.kvproto.Cdcpb;

import java.util.ArrayList;
import java.util.List;

public class RegionStatefulEvent {
    private Cdcpb.Event event;
    private RegionStateManager.RegionFeedState regionFeedState;
    private ResolvedTsEvent resolvedTsEvent;
    private long regionId;

    public RegionStatefulEvent() {
        this.resolvedTsEvent = new RegionStatefulEvent.ResolvedTsEvent();
    }

    // Private constructor to enforce the use of the builder
    private RegionStatefulEvent(Builder builder) {
        this.event = builder.event;
        this.regionFeedState = builder.regionFeedState;
        this.resolvedTsEvent = builder.resolvedTsEvent;
        this.regionId = builder.regionId;
    }

    public static class Builder {
        private Cdcpb.Event event;
        private RegionStateManager.RegionFeedState regionFeedState;
        private ResolvedTsEvent resolvedTsEvent;
        private long regionId;

        public Builder setEvent(Cdcpb.Event event) {
            this.event = event;
            return this;
        }

        public Builder setRegionFeedState(RegionStateManager.RegionFeedState regionFeedState) {
            this.regionFeedState = regionFeedState;
            return this;
        }

        public Builder setResolvedTsEvent(ResolvedTsEvent resolvedTsEvent) {
            this.resolvedTsEvent = resolvedTsEvent;
            return this;
        }

        public Builder setRegionId(long regionId) {
            this.regionId = regionId;
            return this;
        }

        public RegionStatefulEvent build() {
            return new RegionStatefulEvent(this);
        }
    }

    // Getters and Setters
    public Cdcpb.Event getEvent() {
        return event;
    }

    public void setEvent(Cdcpb.Event event) {
        this.event = event;
    }

    public RegionStateManager.RegionFeedState getRegionFeedState() {
        return regionFeedState;
    }

    public void setRegionFeedState(RegionStateManager.RegionFeedState regionFeedState) {
        this.regionFeedState = regionFeedState;
    }

    public ResolvedTsEvent getResolvedTsEvent() {
        return resolvedTsEvent;
    }

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    // Nested ResolvedTsEvent class remains unchanged
    public static class ResolvedTsEvent {
        private long resolvedTs;
        private List<RegionStateManager.RegionFeedState> regions = new ArrayList<>();

        public long getResolvedTs() {
            return resolvedTs;
        }

        public void setResolvedTs(long resolvedTs) {
            this.resolvedTs = resolvedTs;
        }

        public List<RegionStateManager.RegionFeedState> getRegions() {
            return regions;
        }

        public void setRegions(List<RegionStateManager.RegionFeedState> regions) {
            this.regions.addAll(regions);
        }

        @Override
        public String toString() {
            return "ResolvedTsEvent{" + "resolvedTs=" + resolvedTs + ", regions=" + regions + '}';
        }
    }

    @Override
    public String toString() {
        return "RegionStatefulEvent{"
                + "event="
                + event
                + ", regionFeedState="
                + regionFeedState
                + ", resolvedTsEvent="
                + resolvedTsEvent
                + ", regionId="
                + regionId
                + '}';
    }
}

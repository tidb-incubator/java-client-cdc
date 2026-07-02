package org.tikv.cdc.model;

import org.tikv.cdc.kv.RegionStateManager;
import org.tikv.kvproto.Cdcpb;

public class RegionErrorInfo {
    private RegionStateManager.SingleRegionInfo singleRegionInfo;
    private Cdcpb.Error errorCode;

    public RegionErrorInfo(
            RegionStateManager.SingleRegionInfo singleRegionInfo, Cdcpb.Error errorCode) {
        this.singleRegionInfo = singleRegionInfo;
        this.errorCode = errorCode;
    }

    public RegionStateManager.SingleRegionInfo getSingleRegionInfo() {
        return singleRegionInfo;
    }

    public void setSingleRegionInfo(RegionStateManager.SingleRegionInfo singleRegionInfo) {
        this.singleRegionInfo = singleRegionInfo;
    }

    public Cdcpb.Error getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(Cdcpb.Error errorCode) {
        this.errorCode = errorCode;
    }
}

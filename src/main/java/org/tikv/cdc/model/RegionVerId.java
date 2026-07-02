package org.tikv.cdc.model;

import org.tikv.common.region.TiRegion;

/** region Id */
public class RegionVerId {
    final long id;
    final long confVer;
    final long ver;

    public RegionVerId(long id, long confVer, long ver) {
        this.id = id;
        this.confVer = confVer;
        this.ver = ver;
    }

    public long getId() {
        return this.id;
    }

    public long getConfVer() {
        return this.confVer;
    }

    public long getVer() {
        return this.ver;
    }

    public static RegionVerId fromTiRegion(TiRegion.RegionVerID regionVerID) {
        return new RegionVerId(regionVerID.getId(), regionVerID.getConfVer(), regionVerID.getVer());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof RegionVerId)) {
            return false;
        } else {
            RegionVerId that = (RegionVerId) other;
            return this.id == that.id && this.confVer == that.confVer && this.ver == that.ver;
        }
    }

    @Override
    public int hashCode() {
        int hash = Long.hashCode(this.id);
        hash = hash * 31 + Long.hashCode(this.confVer);
        hash = hash * 31 + Long.hashCode(this.ver);
        return hash;
    }

    @Override
    public String toString() {
        return "RegionVerId{" + "id=" + id + ", confVer=" + confVer + ", ver=" + ver + '}';
    }
}

package org.tikv.cdc.frontier;

import org.tikv.cdc.model.RegionKeyRange;

public interface Frontier {
    void forward(long regionID, RegionKeyRange.ComparableKeyRange span, long ts);

    long frontier();

    void entries(FrontierConsumer fn);

    String toString();
}

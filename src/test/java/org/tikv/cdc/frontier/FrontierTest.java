package org.tikv.cdc.frontier;

import org.junit.Assert;
import org.junit.Test;
import org.tikv.cdc.model.RegionKeyRange;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.tikv.cdc.model.RegionKeyRange.UPPER_BOUND_KEY;

public class FrontierTest {

    @Test
    public void keyRangeFrontier() {
        byte[] keyA = new byte[] {'a'};
        byte[] keyB = new byte[] {'b'};
        byte[] keyC = new byte[] {'c'};
        byte[] keyD = new byte[] {'d'};
        RegionKeyRange.ComparableKeyRange krAB = new RegionKeyRange.ComparableKeyRange(keyA, keyB);
        RegionKeyRange.ComparableKeyRange krAC = new RegionKeyRange.ComparableKeyRange(keyA, keyC);
        RegionKeyRange.ComparableKeyRange krAD = new RegionKeyRange.ComparableKeyRange(keyA, keyD);
        RegionKeyRange.ComparableKeyRange krBC = new RegionKeyRange.ComparableKeyRange(keyB, keyC);
        RegionKeyRange.ComparableKeyRange krBD = new RegionKeyRange.ComparableKeyRange(keyB, keyD);
        RegionKeyRange.ComparableKeyRange krCD = new RegionKeyRange.ComparableKeyRange(keyC, keyD);
        Frontier frontier = new KeyRangeFrontier(5, krAD);
        Assert.assertEquals(5, frontier.frontier());
        Assert.assertEquals("[a @ 5] [d @ Max] ", frontier.toString());
        checkFrontier(frontier);
        frontier.forward(
                0, new RegionKeyRange.ComparableKeyRange(new byte[] {'d'}, new byte[] {'e'}), 100);
        Assert.assertEquals(5, frontier.frontier());
        Assert.assertEquals("[a @ 5] [d @ 100] [e @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(
                0, new RegionKeyRange.ComparableKeyRange(new byte[] {'g'}, new byte[] {'h'}), 200);
        Assert.assertEquals(5, frontier.frontier());
        Assert.assertEquals(
                "[a @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(
                0, new RegionKeyRange.ComparableKeyRange(new byte[] {'a'}, new byte[] {'d'}), 1);
        Assert.assertEquals(1, frontier.frontier());
        Assert.assertEquals(
                "[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(
                0, new RegionKeyRange.ComparableKeyRange(new byte[] {'a'}, new byte[] {'d'}), 2);

        Assert.assertEquals(2, frontier.frontier());
        Assert.assertEquals(
                "[a @ 2] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);
        frontier.forward(
                0, new RegionKeyRange.ComparableKeyRange(new byte[] {'a'}, new byte[] {'d'}), 1);
        Assert.assertEquals(1, frontier.frontier());
        Assert.assertEquals(
                "[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krBC, 3);

        Assert.assertEquals(1, frontier.frontier());
        Assert.assertEquals(
                "[a @ 1] [b @ 3] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ",
                frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krBC, 4);
        Assert.assertEquals(1, frontier.frontier());
        Assert.assertEquals(
                "[a @ 1] [b @ 4] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ",
                frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krAD, 3);
        Assert.assertEquals(3, frontier.frontier());
        Assert.assertEquals(
                "[a @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krAB, 5);
        Assert.assertEquals(3, frontier.frontier());
        Assert.assertEquals(
                "[a @ 5] [b @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krCD, 5);
        Assert.assertEquals(3, frontier.frontier());
        Assert.assertEquals(
                "[a @ 5] [b @ 3] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ",
                frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krBC, 5);
        Assert.assertEquals(5, frontier.frontier());
        Assert.assertEquals(
                "[a @ 5] [b @ 5] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ",
                frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krAD, 6);
        Assert.assertEquals(6, frontier.frontier());
        Assert.assertEquals(
                "[a @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krAC, 7);
        Assert.assertEquals(6, frontier.frontier());
        Assert.assertEquals(
                "[a @ 7] [c @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krBD, 8);
        Assert.assertEquals(7, frontier.frontier());
        Assert.assertEquals(
                "[a @ 7] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(0, krAB, 8);
        Assert.assertEquals(8, frontier.frontier());
        Assert.assertEquals(
                "[a @ 8] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(
                0, new RegionKeyRange.ComparableKeyRange(new byte[] {'1'}, new byte[] {'g'}), 9);
        Assert.assertEquals(9, frontier.frontier());
        Assert.assertEquals("[1 @ 9] [g @ 200] [h @ Max] ", frontier.toString());
        checkFrontier(frontier);

        frontier.forward(
                0, new RegionKeyRange.ComparableKeyRange(new byte[] {'g'}, new byte[] {'i'}), 10);
        Assert.assertEquals(9, frontier.frontier());
        Assert.assertEquals("[1 @ 9] [g @ 10] [i @ Max] ", frontier.toString());
        checkFrontier(frontier);
    }

    @Test
    public void keyRangeFrontierCPK() {
        RegionKeyRange.ComparableKeyRange init =
                new RegionKeyRange.ComparableKeyRange(new byte[0], UPPER_BOUND_KEY);
        Frontier frontier = new KeyRangeFrontier(0, init);
        byte[] keyA = new byte[] {'a'};
        byte[] keyB = new byte[] {'b'};
        frontier.forward(1, new RegionKeyRange.ComparableKeyRange(keyA, keyB), 10);
        // The untouched portions outside [a, b) remain at the initial checkpoint.
        Assert.assertEquals(0, frontier.frontier());
    }

    @Test
    public void keyRangFrontierFallback() {
        byte[] keyA = new byte[] {'a'};
        byte[] keyB = new byte[] {'b'};
        byte[] keyC = new byte[] {'c'};
        byte[] keyD = new byte[] {'d'};
        byte[] keyE = new byte[] {'e'};

        RegionKeyRange.ComparableKeyRange krAB = new RegionKeyRange.ComparableKeyRange(keyA, keyB);
        RegionKeyRange.ComparableKeyRange krBC = new RegionKeyRange.ComparableKeyRange(keyB, keyC);
        RegionKeyRange.ComparableKeyRange krCD = new RegionKeyRange.ComparableKeyRange(keyC, keyD);
        RegionKeyRange.ComparableKeyRange krDE = new RegionKeyRange.ComparableKeyRange(keyD, keyE);

        Frontier frontier = new KeyRangeFrontier(20, krAB);
        frontier.forward(0, krBC, 20);
        frontier.forward(0, krCD, 10);
        frontier.forward(0, krDE, 20);
        Assert.assertEquals(10, frontier.frontier());
        Assert.assertEquals("[a @ 20] [b @ 20] [c @ 10] [d @ 20] [e @ Max] ", frontier.toString());

        checkFrontier(frontier);

        frontier.forward(0, krCD, 20);
        Assert.assertEquals(20, frontier.frontier());

        Assert.assertEquals("[a @ 20] [b @ 20] [c @ 20] [d @ 20] [e @ Max] ", frontier.toString());
        checkFrontier(frontier);
    }

    @Test
    public void MinMaxFrontier() {
        byte[] keyMin = new byte[] {};
        byte[] keyMax = UPPER_BOUND_KEY;
        byte[] keyMid = new byte[] {'m'};
        RegionKeyRange.ComparableKeyRange keyMinMid =
                new RegionKeyRange.ComparableKeyRange(keyMin, keyMid);
        keyMinMid.hack();
        RegionKeyRange.ComparableKeyRange keyMidMax =
                new RegionKeyRange.ComparableKeyRange(keyMid, keyMax);
        keyMidMax.hack();
        RegionKeyRange.ComparableKeyRange keyMinMax =
                new RegionKeyRange.ComparableKeyRange(keyMin, keyMax);
        keyMinMax.hack();
        Frontier frontier = new KeyRangeFrontier(0, keyMinMax);
        Assert.assertEquals(0, frontier.frontier());
        Assert.assertEquals(
                String.format(
                        "[ @ 0] [%s @ Max] ", new String(UPPER_BOUND_KEY, StandardCharsets.UTF_8)),
                frontier.toString());
        frontier.forward(0, keyMinMax, 1);
        Assert.assertEquals(1, frontier.frontier());
        checkFrontier(frontier);
        frontier.forward(0, keyMinMid, 2);
        Assert.assertEquals(1, frontier.frontier());
        //        checkFrontier(frontier);

        frontier.forward(0, keyMidMax, 2);
        Assert.assertEquals(2, frontier.frontier());
        checkFrontier(frontier);

        frontier.forward(0, keyMinMax, 3);
        Assert.assertEquals(3, frontier.frontier());
        checkFrontier(frontier);
    }

    private void checkFrontier(Frontier f) {
        KeyRangeFrontier sf = (KeyRangeFrontier) f;
        List<Long> tsInlist = new ArrayList<>();
        List<Long> tsInHeap = new ArrayList<>();
        sf.getSpanList()
                .entries(
                        (n) -> {
                            tsInlist.add(n.value.getKey());
                            return true;
                        });
        sf.getMinTsHeap()
                .entries(
                        (n) -> {
                            tsInHeap.add(n.getKey());
                            return true;
                        });
        Assert.assertEquals(tsInlist.size(), tsInHeap.size());
        Collections.sort(tsInlist);
        Collections.sort(tsInHeap);
        Assert.assertEquals(Optional.ofNullable(tsInlist.get(0)), Optional.of(f.frontier()));
    }
}

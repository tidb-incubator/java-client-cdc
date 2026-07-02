package org.tikv.cdc.model;

import org.junit.Assert;
import org.junit.Test;

/** region key Range Test */
public class RegionKeyRangeTest {
    @Test
    public void startCompareTest() {
        RegionKeyRange.ComparableKeyRange keyRange1 =
                new RegionKeyRange.ComparableKeyRange(null, null);
        int res1 = RegionKeyRange.startCompare(keyRange1.getStart(), keyRange1.getEnd());
        Assert.assertEquals(0, res1);
        RegionKeyRange.ComparableKeyRange keyRange2 =
                new RegionKeyRange.ComparableKeyRange(null, new byte[] {});
        int res2 = RegionKeyRange.startCompare(keyRange2.getStart(), keyRange2.getEnd());
        Assert.assertEquals(-1, res2);

        RegionKeyRange.ComparableKeyRange keyRange3 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {1}, new byte[] {2});
        int res3 = RegionKeyRange.startCompare(keyRange3.getStart(), keyRange3.getEnd());
        Assert.assertEquals(-1, res3);

        RegionKeyRange.ComparableKeyRange keyRange4 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {2}, new byte[] {1});
        int res4 = RegionKeyRange.startCompare(keyRange4.getStart(), keyRange4.getEnd());
        Assert.assertEquals(1, res4);
    }

    @Test
    public void endCompareTest() {
        RegionKeyRange.ComparableKeyRange keyRange1 =
                new RegionKeyRange.ComparableKeyRange(null, null);
        int res1 = RegionKeyRange.endCompare(keyRange1.getStart(), keyRange1.getEnd());
        Assert.assertEquals(0, res1);
        RegionKeyRange.ComparableKeyRange keyRange2 =
                new RegionKeyRange.ComparableKeyRange(null, new byte[] {});
        int res2 = RegionKeyRange.endCompare(keyRange2.getStart(), keyRange2.getEnd());
        Assert.assertEquals(1, res2);

        RegionKeyRange.ComparableKeyRange keyRange3 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {1}, new byte[] {2});
        int res3 = RegionKeyRange.endCompare(keyRange3.getStart(), keyRange3.getEnd());
        Assert.assertEquals(-1, res3);

        RegionKeyRange.ComparableKeyRange keyRange4 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {2}, new byte[] {1});
        int res4 = RegionKeyRange.endCompare(keyRange4.getStart(), keyRange4.getEnd());
        Assert.assertEquals(1, res4);
    }

    @Test
    public void intersectTest() throws Exception {
        // 有交集情况：lhs.getStart() 小于 rhs.getStart(), lhs.getEnd() 大于 rhs.getEnd()
        RegionKeyRange.ComparableKeyRange lhs1 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {0}, new byte[] {3});
        RegionKeyRange.ComparableKeyRange rhs1 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {1}, new byte[] {2});
        RegionKeyRange.ComparableKeyRange result1 = RegionKeyRange.intersect(lhs1, rhs1);
        Assert.assertArrayEquals(new byte[] {1}, result1.getStart());
        Assert.assertArrayEquals(new byte[] {2}, result1.getEnd());

        RegionKeyRange.ComparableKeyRange lhs2 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {0}, new byte[] {2});
        RegionKeyRange.ComparableKeyRange rhs2 =
                new RegionKeyRange.ComparableKeyRange(new byte[] {1}, new byte[] {2});
        RegionKeyRange.ComparableKeyRange result2 = RegionKeyRange.intersect(lhs2, rhs2);
        Assert.assertArrayEquals(new byte[] {1}, result2.getStart());
        Assert.assertArrayEquals(new byte[] {2}, result2.getEnd());
    }
}

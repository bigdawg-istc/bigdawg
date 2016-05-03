package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Created by ankush on 4/25/16.
 */
public class Histogram {
    private final long[] buckets;
    private final Map<Long, Long> hotspots;
    private final BinaryJoinExecutionNode.JoinOperand operand;
    private final long min, max;

    public Histogram(long[] buckets, Map<Long, Long> hotspots, Long min, Long max, BinaryJoinExecutionNode.JoinOperand operand) {
        this.buckets = buckets;
        this.hotspots = hotspots;
        this.operand = operand;
        this.min = min;
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public BinaryJoinExecutionNode.JoinOperand getOperand() {
        return operand;
    }

    public long getBucketCount(int bucket) {
        return buckets[bucket];
    }

    public Collection<Long> getHotspots() {
        return hotspots.keySet();
    }

    public long getHotspotCount(long hotspot) {
        return hotspots.get(hotspot);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Histogram)) return false;

        Histogram histogram = (Histogram) o;

        if (min != histogram.min) return false;
        if (max != histogram.max) return false;
        if (!Arrays.equals(buckets, histogram.buckets)) return false;
        if (!hotspots.equals(histogram.hotspots)) return false;
        return operand.equals(histogram.operand);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(buckets);
        result = 31 * result + hotspots.hashCode();
        result = 31 * result + operand.hashCode();
        result = 31 * result + (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        return result;
    }
}

package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

import java.util.Collection;
import java.util.Map;

/**
 * Created by ankush on 4/25/16.
 */
public class Histogram {
    private final long[] buckets;
    private final Map<Long, Long> hotspots;
    private final BinaryJoinExecutionNode.JoinOperand operand;
    private final Long min, max;

    public Histogram(long[] buckets, Map<Long, Long> hotspots, Long min, Long max, BinaryJoinExecutionNode.JoinOperand operand) {
        this.buckets = buckets;
        this.hotspots = hotspots;
        this.operand = operand;
        this.min = min;
        this.max = max;
    }

    public Long getMin() {
        return min;
    }

    public Long getMax() {
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
}

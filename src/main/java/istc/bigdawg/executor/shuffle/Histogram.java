package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

/**
 * Created by ankush on 4/25/16.
 */
public class Histogram {
    private final long[] buckets;
    private final BinaryJoinExecutionNode.JoinOperand operand;
    private final double min, max;

    public Histogram(long[] data, double min, double max, BinaryJoinExecutionNode.JoinOperand operand) {
        this.buckets = data;
        this.operand = operand;
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public BinaryJoinExecutionNode.JoinOperand getOperand() {
        return operand;
    }

    public long getCount(int bucket) {
        return buckets[bucket];
    }
}

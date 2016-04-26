package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

/**
 * Created by ankush on 4/25/16.
 */
public class Histogram {
    private final long[] buckets;
    public final BinaryJoinExecutionNode.JoinOperand operand;

    public Histogram(long[] data, BinaryJoinExecutionNode.JoinOperand operand) {
        this.buckets = data;
        this.operand = operand;
    }

    public long getCount(int bucket) {
        return buckets[bucket];
    }
}

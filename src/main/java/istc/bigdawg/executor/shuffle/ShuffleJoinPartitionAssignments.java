package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ankush on 4/25/16.
 */
public class ShuffleJoinPartitionAssignments {
    public final double min;
    public final double max;
    private final BinaryJoinExecutionNode.JoinOperand[] assignments;

    public ShuffleJoinPartitionAssignments(double min, double max, int bucketCount) {
        this.min = min;
        this.max = max;

        assignments = new BinaryJoinExecutionNode.JoinOperand[bucketCount];
    }

    public Collection<Integer> getBucketsForJoinOperand(BinaryJoinExecutionNode.JoinOperand operand) {
        return IntStream.range(0, assignments.length)
                .filter(i -> assignments[i].equals(operand))
                .boxed()
                .collect(Collectors.toSet());
    }

    public BinaryJoinExecutionNode.JoinOperand getAssignment(int bucket) {
        return assignments[bucket];
    }

    private void assignBucket(int bucket, BinaryJoinExecutionNode.JoinOperand operand) {
        assignments[bucket] = operand;
    }
}

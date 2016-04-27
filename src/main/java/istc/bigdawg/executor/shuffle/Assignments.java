package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ankush on 4/25/16.
 */
public class Assignments {
    private final double min;
    private final double max;
    private final BinaryJoinExecutionNode.JoinOperand[] assignments;

    public static Assignments minimizeBandwidth(Collection<Histogram> histograms, int numBuckets) {
        double min = histograms.stream().mapToDouble(Histogram::getMin).min().getAsDouble();
        double max = histograms.stream().mapToDouble(Histogram::getMax).max().getAsDouble();

        Assignments assignments = new Assignments(min, max, numBuckets);

        IntStream.range(0, numBuckets).forEach(
                bucket -> histograms.stream().max((Comparator.comparingLong((Histogram histogram) -> histogram.getCount(bucket))))
                        .ifPresent(histogram -> assignments.assignBucket(bucket, histogram.getOperand())));

        return assignments;
    }

    public Assignments(double min, double max, int bucketCount) {
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

    public double getMinValue() {
        return min;
    }

    public double getMaxValue() {
        return max;
    }

    public BinaryJoinExecutionNode.JoinOperand getAssignment(int bucket) {
        return assignments[bucket];
    }

    public void assignBucket(int bucket, BinaryJoinExecutionNode.JoinOperand operand) {
        assignments[bucket] = operand;
    }
}

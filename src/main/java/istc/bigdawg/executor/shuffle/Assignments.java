package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ankush on 4/25/16.
 */
public class Assignments {
    public enum AssignmentStrategies {
        MINIMUM_BANDWIDTH,
        REBALANCE_HOTSPOT,
        REBALANCE_BUCKETS
    }

    private final long min;
    private final long max;
    private final BinaryJoinExecutionNode.JoinOperand[] bucketAssignments;
    private final Map<Long, BinaryJoinExecutionNode.JoinOperand> hotspotAssignments;

    public static Assignments assignTuples(Collection<Histogram> histograms, int numBuckets, AssignmentStrategies strat) {
        switch(strat) {
            case MINIMUM_BANDWIDTH:
                return minimizeBandwidth(histograms, numBuckets);
            case REBALANCE_BUCKETS:
                return rebalanceBuckets(histograms, numBuckets);
            case REBALANCE_HOTSPOT:
                return rebalanceHotspots(histograms, numBuckets);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static Assignments minimizeBandwidth(Collection<Histogram> histograms, int numBuckets) {
        long min = histograms.stream().mapToLong(Histogram::getMin).min().getAsLong();
        long max = histograms.stream().mapToLong(Histogram::getMax).max().getAsLong();

        Assignments assignments = new Assignments(min, max, numBuckets);

        IntStream.range(0, numBuckets).forEach(
                bucket -> histograms.stream().max((Comparator.comparingLong((Histogram histogram) -> histogram.getBucketCount(bucket))))
                        .ifPresent(histogram -> assignments.assignBucket(bucket, histogram.getOperand())));

        histograms.stream().flatMap(h -> h.getHotspots().stream()).forEach(t -> {
                    histograms.stream().max(Comparator.comparingLong(h -> h.getHotspotCount(t)))
                            .ifPresent(h -> assignments.assignHotspot(t, h.getOperand()));
                });

        return assignments;
    }

    public static Assignments rebalanceBuckets(Collection<Histogram> histograms, int numBuckets) {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static Assignments rebalanceHotspots(Collection<Histogram> histograms, int numBuckets) {
        // TODO
        throw new UnsupportedOperationException();
    }

    public Assignments(long min, long max, int bucketCount) {
        this.min = min;
        this.max = max;

        hotspotAssignments = new HashMap<>();
        bucketAssignments = new BinaryJoinExecutionNode.JoinOperand[bucketCount];
    }

    public Collection<Integer> getBucketsForJoinOperand(BinaryJoinExecutionNode.JoinOperand operand) {
        return IntStream.range(0, bucketAssignments.length)
                .filter(i -> bucketAssignments[i].equals(operand))
                .boxed()
                .collect(Collectors.toSet());
    }

    public long getMinValue() {
        return min;
    }

    public long getMaxValue() {
        return max;
    }

    public BinaryJoinExecutionNode.JoinOperand assignHotspot(long hotspot, BinaryJoinExecutionNode.JoinOperand operand) {
        return hotspotAssignments.put(hotspot, operand);
    }

    public BinaryJoinExecutionNode.JoinOperand getAssignment(int bucket) {
        return bucketAssignments[bucket];
    }

    public void assignBucket(int bucket, BinaryJoinExecutionNode.JoinOperand operand) {
        bucketAssignments[bucket] = operand;
    }
}

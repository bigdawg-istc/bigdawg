package istc.bigdawg.executor.shuffle;

import com.google.common.collect.Sets;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.query.ConnectionInfo;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by ankush on 4/25/16.
 */
public class Assignments {
    public enum AssignmentStrategies {
        MINIMUM_BANDWIDTH,
        TABU_SEARCH
    }

    private final long min;
    private final long max;
    private final int bucketCount;
    private final Collection<Histogram> histograms;
    private final BinaryJoinExecutionNode.JoinOperand[] bucketAssignments;
    private final Map<Long, BinaryJoinExecutionNode.JoinOperand> hotspotAssignments;
    private final ConnectionInfo fallback;
    private final double stepSize;

    public static Assignments assignTuples(Collection<Histogram> histograms, ConnectionInfo fallback, int numBuckets, AssignmentStrategies strat) {
        switch(strat) {
            case MINIMUM_BANDWIDTH:
                return getMinimumBandwidthAssignment(histograms, fallback, numBuckets);
            case TABU_SEARCH:
                return getTabuSearchAssignment(histograms, fallback, numBuckets);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static Assignments getMinimumBandwidthAssignment(Collection<Histogram> histograms, ConnectionInfo fallback, int numBuckets) {
        Assignments assignments = new Assignments(numBuckets, histograms, fallback);

        // assign buckets to center of mass
        IntStream.range(0, numBuckets)
                .forEach(bucket -> histograms.stream().max((Comparator.comparingLong((Histogram histogram) -> histogram.getBucketCount(bucket))))
                        .ifPresent(histogram -> assignments.assignBucket(bucket, histogram.getOperand())));

        // assign hotspots to center of mass
        histograms.stream().flatMap(h -> h.getHotspots().stream())
                .forEach(t -> histograms.stream().max(Comparator.comparingLong(h -> h.getHotspotCount(t)))
                .ifPresent(h -> assignments.assignHotspot(t, h.getOperand())));

        return assignments;
    }

    public static Assignments getTabuSearchAssignment(Collection<Histogram> histograms, ConnectionInfo fallback, int numBuckets) {
        Assignments current = getMinimumBandwidthAssignment(histograms, fallback, numBuckets);

        Collection<Pair<Histogram, JoinUnit>> tabuList = new HashSet<>();
        for (Histogram h : histograms) {
            for(JoinUnit u : current.getJoinUnitsForJoinOperand(h.getOperand())) {
                tabuList.add(ImmutablePair.of(h, u));
            }
        }

        Assignments prev = null;
        while (!current.equals(prev)) {
            Map<Histogram, Double> perNodeCost = current.getPerNodeCost();
            double rebalanceThreshold = perNodeCost.values().stream().mapToDouble(Double::doubleValue).average().getAsDouble();

            prev = current;

            for(Histogram h : perNodeCost.keySet()) {
                if (perNodeCost.get(h) >= rebalanceThreshold) {
                    current = current.rebalanceAssignmentsForNode(h, tabuList);
                }
            }
        }

        return current;
    }

    private Assignments rebalanceAssignmentsForNode(Histogram h, Collection<Pair<Histogram, JoinUnit>> tabuList) {
        // TODO: get this
        Assignments best = this;

        for (JoinUnit u : this.getJoinUnitsForJoinOperand(h.getOperand())) {
            for (Histogram candidate : this.histograms) {
                if (!candidate.equals(h) && !tabuList.contains(ImmutablePair.of(candidate, u))) {
                    Assignments working = Assignments.copyOf(this);
                    working.assignJoinUnit(u, candidate);
                    if(working.getCost() <= this.getCost()) {
                        best = working;
                        tabuList.add(ImmutablePair.of(candidate, u));
                    }
                }
            }
        }

        return best;
    }

    private static Assignments copyOf(Assignments original) {
        return new Assignments(original.bucketCount, new HashSet<>(original.histograms), original.fallback, Arrays.copyOf(original.bucketAssignments, original.bucketCount), new HashMap(original.hotspotAssignments));
    }

    private Assignments(int bucketCount, Collection<Histogram> histograms, ConnectionInfo fallback, BinaryJoinExecutionNode.JoinOperand[] bucketAssignments, Map<Long, BinaryJoinExecutionNode.JoinOperand> hotspotAssignments) {
        this.min = histograms.stream().mapToLong(Histogram::getMin).min().getAsLong();
        this.max = histograms.stream().mapToLong(Histogram::getMax).max().getAsLong();
        this.bucketCount = bucketCount;
        this.histograms = histograms;
        this.fallback = fallback;

        this.hotspotAssignments = hotspotAssignments;
        this.bucketAssignments = bucketAssignments;
        stepSize = (max - min) * 1.0 / bucketCount;
    }

    private void assignJoinUnit(JoinUnit u, Histogram candidate) {
        if (u instanceof Range) {
            this.assignBucket(getIndexForRange((Range) u), candidate.getOperand());
        } else if (u instanceof Hotspot) {
            this.assignHotspot(((Hotspot) u).val, candidate.getOperand());
        } else {
            throw new IllegalArgumentException();
        }
    }

    public Assignments(int bucketCount, Collection<Histogram> histograms, ConnectionInfo fallback) {
        this.min = histograms.stream().mapToLong(Histogram::getMin).min().getAsLong();
        this.max = histograms.stream().mapToLong(Histogram::getMax).max().getAsLong();
        this.bucketCount = bucketCount;
        this.histograms = histograms;
        this.fallback = fallback;

        hotspotAssignments = new HashMap<>();
        bucketAssignments = new BinaryJoinExecutionNode.JoinOperand[bucketCount];
        stepSize = (max - min) * 1.0 / bucketCount;
    }

    public Set<Range> getRangesForJoinOperand(BinaryJoinExecutionNode.JoinOperand operand) {

        Collection<Range> extremities = Collections.emptySet();
        if (operand.engine.equals(fallback)) {
            extremities = Arrays.asList(JoinUnit.of(-Double.MIN_VALUE, min), JoinUnit.of(max, -Double.MAX_VALUE));
        }

        return Stream.concat(extremities.stream(),
                IntStream.range(0, bucketAssignments.length)
                        .filter(i -> bucketAssignments[i].equals(operand))
                        .mapToObj(this::getRangeForIndex)
        ).collect(Collectors.toSet());
    }

    public Set<Hotspot> getHotspotsForJoinOperand(BinaryJoinExecutionNode.JoinOperand operand) {
        return hotspotAssignments.entrySet().stream()
                .filter(e -> e.getValue().equals(operand))
                .map(e -> JoinUnit.of(e.getKey()))
                .collect(Collectors.toSet());
    }

    public Set<Hotspot> getHotspotsNotForJoinOperand(BinaryJoinExecutionNode.JoinOperand operand) {
        return hotspotAssignments.entrySet().stream()
                .filter(e -> !e.getValue().equals(operand))
                .map(e-> JoinUnit.of(e.getKey()))
                .collect(Collectors.toSet());
    }

    public Collection<JoinUnit> getJoinUnitsForJoinOperand(BinaryJoinExecutionNode.JoinOperand operand) {
        return Sets.union(this.getRangesForJoinOperand(operand), this.getHotspotsForJoinOperand(operand));
    }

    public double getCost() {
        return this.getPerNodeCost().values().stream().mapToDouble(Double::valueOf).max().getAsDouble();
    }

    public Map<Histogram, Double> getPerNodeCost() {
        Map<Histogram, Double> result = new HashMap<>();

        for (Histogram h : histograms) {
            Map<Histogram, Long> inboundPerEngine = new HashMap<>();

            for(Histogram source : histograms) {
                long count = 0;
                for (JoinUnit u : this.getJoinUnitsForJoinOperand(h.getOperand())) {
                    if (u instanceof Range) {
                        count += source.getBucketCount(this.getIndexForRange((Range) u));
                    } else if (u instanceof Hotspot) {
                        count += source.getHotspotCount(((Hotspot) u).val);
                    }
                }

                inboundPerEngine.put(source, count);
            }

            double maxAlignmentTime = inboundPerEngine.entrySet().stream()
                    .filter(e -> !e.getKey().equals(h))
                    .flatMapToDouble(e -> DoubleStream.of(
//                            e.getKey().getOperand().engine.computeUplinkCost(e.getValue()),
                            ShuffleEngine.computeUplinkCost(e.getValue()),
//                            h.getOperand().engine.computeDownlinkCost(e.getValue())
                            ShuffleEngine.computeDownlinkCost(e.getValue())
                    )).max().getAsDouble();

//            double comparisonTime = h.computeComparisonCost(inboundPerEngine.values().stream().mapToLong(Long::longValue).sum());
            double comparisonTime = ShuffleEngine.computeComparisonCost(inboundPerEngine.values().stream().mapToLong(Long::longValue).sum());

            result.put(h, maxAlignmentTime + comparisonTime);
        }

        return result;
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

    private int getIndexForRange(Range r) {
        return (int) ((r.start - min) / stepSize);
    }

    public Range getRangeForIndex(int i) {
        return JoinUnit.of(min + i * stepSize, min + 2 * i * stepSize);
    }

    public BinaryJoinExecutionNode.JoinOperand getAssignment(int bucket) {
        return bucketAssignments[bucket];
    }

    public void assignBucket(int bucket, BinaryJoinExecutionNode.JoinOperand operand) {
        bucketAssignments[bucket] = operand;
    }

    public ConnectionInfo getFallbackNode() {
        return fallback;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Assignments)) return false;

        Assignments that = (Assignments) o;

        if (min != that.min) return false;
        if (max != that.max) return false;
        if (bucketCount != that.bucketCount) return false;
        if (!histograms.equals(that.histograms)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(bucketAssignments, that.bucketAssignments)) return false;
        return hotspotAssignments.equals(that.hotspotAssignments);

    }

    @Override
    public int hashCode() {
        int result = (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        result = 31 * result + bucketCount;
        result = 31 * result + histograms.hashCode();
        result = 31 * result + Arrays.hashCode(bucketAssignments);
        result = 31 * result + hotspotAssignments.hashCode();
        return result;
    }

    public interface JoinUnit {
        static Hotspot of(long l) {
            return new Hotspot(l);
        }

        static Range of(double start, double end) {
            return new Range(start, end);
        }
    }

    public static class Hotspot implements JoinUnit {
        final long val;

        public Hotspot(long val) {
            this.val = val;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Hotspot)) return false;

            Hotspot hotspot = (Hotspot) o;

            return val == hotspot.val;
        }

        @Override
        public int hashCode() {
            return (int) (val ^ (val >>> 32));
        }

    }

    public static class Range implements JoinUnit {
        final double start;
        final double end;

        public Range(double start, double end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Range)) return false;

            Range range = (Range) o;

            if (Double.compare(range.start, start) != 0) return false;
            return Double.compare(range.end, end) == 0;

        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = Double.doubleToLongBits(start);
            result = (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(end);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
    }
}

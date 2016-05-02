package istc.bigdawg.executor.shuffle;

import com.diffplug.common.base.Errors;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.query.ConnectionInfo;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ankush on 4/25/16.
 */
public class ShuffleEngine {
    public static final int NUM_BUCKETS = 100;
    private static final String PG_STATS_PREP_TEMPLATE = "ANALYZE %s %s";
    private static final String PG_STATS_TEMPLATE = "SELECT array_to_json(most_common_vals) AS most_common_vals, array_to_json(most_common_freqs) AS most_common_freqs, array_to_json(histogram_bounds) AS histogram_bounds, approximate_row_count AS count FROM pg_stats JOIN pg_class ON relname = tablename WHERE tablename = '%s' AND attname = '%s';";

    enum HistogramStrategy {
        EXHAUSTIVE,
        SAMPLING
    }

    public static Collection<Histogram> createHistograms(Collection<BinaryJoinExecutionNode.JoinOperand> operands, HistogramStrategy strategy) throws ConnectionInfo.LocalQueryExecutorLookupException, ExecutorEngine.LocalQueryExecutionException {
        switch(strategy) {
            case EXHAUSTIVE:
                return createHistogramsExhaustively(operands);
            case SAMPLING:
                return createHistogramsBySampling(operands);
            default:
                throw new IllegalArgumentException();
        }
    }

    static Collection<Histogram> createHistogramsExhaustively(Collection<BinaryJoinExecutionNode.JoinOperand> operands) {
        Stream<Pair<Long, Long>> minMaxStream =  operands.stream()
                .map(Errors.rethrow().wrap((BinaryJoinExecutionNode.JoinOperand o) -> o.engine.getMinMax(o.table, o.attribute)))
                .map(p -> new ImmutablePair<>(p.getLeft().longValue(), p.getRight().longValue()));

        long min = minMaxStream.mapToLong(Pair::getLeft).min().getAsLong();
        long max = minMaxStream.mapToLong(Pair::getRight).max().getAsLong();

        return operands.stream()
                .map(Errors.rethrow().wrap((BinaryJoinExecutionNode.JoinOperand o) -> new Histogram(o.engine.computeHistogram(o.table, o.attribute, min, max, NUM_BUCKETS), Collections.emptyMap(), min, max, o)))
                .collect(Collectors.toSet());
    }

    static Collection<Histogram> createHistogramsBySampling(Collection<BinaryJoinExecutionNode.JoinOperand> operands) throws ConnectionInfo.LocalQueryExecutorLookupException, ExecutorEngine.LocalQueryExecutionException {
        Map<BinaryJoinExecutionNode.JoinOperand, Map<Long, Long>> perOperandCommonValCounts = new HashMap<>();
        Map<BinaryJoinExecutionNode.JoinOperand, List<Long>> perOperandHistogramBounds = new HashMap<>();
        Map<BinaryJoinExecutionNode.JoinOperand, Long> perOperandHistogramBucketCount = new HashMap<>();

        // TODO: parallelize this
        for(BinaryJoinExecutionNode.JoinOperand o : operands) {
            ExecutorEngine e = o.engine.getLocalQueryExecutor();
            e.execute(String.format(PG_STATS_PREP_TEMPLATE, o.table, o.attribute));
            JdbcQueryResult r = (JdbcQueryResult) e.execute(String.format(PG_STATS_TEMPLATE, o.table, o.attribute)).get();
            List<String> row = r.getRows().get(0);

            Long count = Long.valueOf(row.get(4));
            Long hotspotTotal = 0l;

            Map<Long, Long> commonValCounts = new HashMap<>();
            Iterator<Number> ids = ((JSONArray) JSONValue.parse(row.get(0))).iterator();
            Iterator<Object> freqs = ((JSONArray) JSONValue.parse(row.get(1))).iterator();
            while (ids.hasNext() || freqs.hasNext()) {
                long hotspotCount = (long) (Double.parseDouble(freqs.next().toString()) * count);
                commonValCounts.put(Long.parseLong(ids.next().toString()), hotspotCount);
                hotspotTotal += count;
            }
            perOperandCommonValCounts.put(o, commonValCounts);

            List<Long> histogramBounds = ((List<Object>) JSONValue.parse(row.get(3))).stream()
                    .map(v -> Long.parseLong(v.toString())).collect(Collectors.toList());
            perOperandHistogramBounds.put(o, histogramBounds);

            perOperandHistogramBucketCount.put(o, (count - hotspotTotal) / histogramBounds.size());
        }

        long globalMin = perOperandHistogramBounds.values().stream().filter(l -> !l.isEmpty()).mapToLong(l -> l.get(0)).min().getAsLong();
        long globalMax = perOperandHistogramBounds.values().stream().filter(l -> !l.isEmpty()).mapToLong(l -> l.get(l.size())).max().getAsLong();

        return operands.stream()
                .map(o -> new Histogram(rescaleHistogramBounds(perOperandHistogramBounds.get(o), globalMin, globalMax, perOperandHistogramBucketCount.get(o), NUM_BUCKETS), perOperandCommonValCounts.get(o), globalMin, globalMax, o))
                .collect(Collectors.toList());
    }

    private static long[] rescaleHistogramBounds(List<Long> bounds, Long min, Long max, long boundWidth, int numBuckets) {
        double stepSize = (max - min) * 1.0 / numBuckets;
        long[] rescaled = new long[numBuckets];

        int boundIndex = 0;
        for(int i = 0; i < numBuckets && boundIndex < bounds.size(); i++) {
            long curCount = 0;

            // possibly have portion of the previous bucket to account for still
            if (boundIndex > 1) {
                double leftOver = (1.0 - (1.0 * min + 2 * i * stepSize - bounds.get(boundIndex - 1)) / (bounds.get(boundIndex) - bounds.get(boundIndex - 1))) * boundIndex;
                if (leftOver > 0) {
                    curCount += (long) leftOver;
                }
            }

            // all following buckets are completely contained
            while(bounds.get(boundIndex) < min + 2 * i * stepSize) {
                curCount += boundWidth;
                boundIndex++;
            }

            if (boundIndex < bounds.size() - 1) {
                // current bucket (boundIndex to boundIndex + 1) is only partially contained
                double overflow = (1.0 * min + 2 * i * stepSize - bounds.get(boundIndex)) / (bounds.get(boundIndex + 1) - bounds.get(boundIndex)) * boundIndex;
                if (overflow > 0) {
                    curCount += (long) overflow;
                }
            }

            rescaled[i] = curCount;
        }

        return rescaled;
    }
}

package istc.bigdawg.executor;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ShuffleJoinExecutor {
    private BinaryJoinExecutionNode node;
    private double min;
    private double max;
    private static final int NUM_BUCKETS = 10;

    public ShuffleJoinExecutor(BinaryJoinExecutionNode node) throws SQLException, ParseException {
        this.node = node;

        BinaryJoinExecutionNode.JoinOperand left = node.getLeft();
        BinaryJoinExecutionNode.JoinOperand right = node.getRight();
        Pair<Number, Number> mmLeft = left.engine.getMinMax(left.table, left.attribute);
        Pair<Number, Number> mmRight = right.engine.getMinMax(right.table, right.attribute);

        this.min = Math.min(mmLeft.getLeft().doubleValue(), mmRight.getRight().doubleValue()) - 1;
        this.max = Math.max(mmLeft.getRight().doubleValue(), mmRight.getRight().doubleValue()) + 1;
    }

    private class Histogram {
        private final long[] buckets;
        private final BinaryJoinExecutionNode.JoinOperand operand;

        public Histogram(long[] data, BinaryJoinExecutionNode.JoinOperand operand) {
            this.buckets = data;
            this.operand = operand;
        }

        public long getCount(int bucket) {
            return buckets[bucket];
        }
    }

    private class ShuffleJoinPartitionAssignments {
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

    private Histogram extractHistogram(BinaryJoinExecutionNode.JoinOperand operand) throws SQLException {
        return new Histogram(operand.engine.computeHistogram(operand.table, operand.attribute, this.min, this.max, NUM_BUCKETS), operand);
    }

    private ShuffleJoinPartitionAssignments assignBuckets(Histogram... histograms) {
        ShuffleJoinPartitionAssignments assignments = new ShuffleJoinPartitionAssignments(this.min, this.max, NUM_BUCKETS);
        IntStream.range(0, NUM_BUCKETS).forEach(
                bucket -> Arrays.stream(histograms).max((Comparator.comparingLong((Histogram histogram) -> histogram.getCount(bucket))))
                        .ifPresent(histogram -> assignments.assignBucket(bucket, histogram.operand)));
        return assignments;
    }

    private String getPartitionQuery(BinaryJoinExecutionNode.JoinOperand operand, Collection<Integer> bucketsToInclude, String dest) {
        String query = "SELECT * INTO %s FROM %s WHERE %s IN %s;";
        String desiredBuckets = "(" + bucketsToInclude.stream().map(i -> i.toString()).collect(Collectors.joining(", ")) + ")";
        return String.format(query, dest, operand.table, operand.attribute, desiredBuckets);
    }

    private QueryExecutionPlan createQueryExecutionPlan(ShuffleJoinPartitionAssignments assignments) {
        // TODO: create LocalQueryExecutionNode to perform getPartitionQuery(left, assignments.getBucketsForJoinOperand(right), "sendToRight")
        // TODO: create LocalQueryExecutionNode to perform getPartitionQuery(right, assignments.getBucketsForJoinOperand(left), "sendToLeft")

        // TODO: create LocalQueryExecutionNode to perform left.getQueryString(sendToLeft, "leftResults")
        // TODO: create LocalQueryExecutionNode to perform right.getQueryString(sendToRight, "rightResults")

        // TODO: SELECT * INTO node.getTableName().get() FROM leftResults UNION ALL SELECT * FROM rightResults
        return null;
    }

    public Optional<PostgreSQLHandler.QueryResult> execute() throws SQLException, MigrationException {
        Histogram leftDistribution = extractHistogram(node.getLeft());
        Histogram rightDistribution = extractHistogram(node.getRight());

        ShuffleJoinPartitionAssignments assignments = this.assignBuckets(leftDistribution, rightDistribution);

        return Optional.ofNullable(Executor.executePlan(this.createQueryExecutionPlan(assignments)));
    }
}

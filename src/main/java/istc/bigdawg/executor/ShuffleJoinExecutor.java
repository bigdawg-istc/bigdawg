package istc.bigdawg.executor;

import com.google.common.collect.Sets;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.executor.plan.LocalQueryExecutionNode;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.utils.IslandsAndCast;
import org.apache.commons.lang3.tuple.Pair;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ShuffleJoinExecutor {
    private BinaryJoinExecutionNode node;
    private static final int NUM_BUCKETS = 100;

    public ShuffleJoinExecutor(BinaryJoinExecutionNode node) throws ExecutorEngine.LocalQueryExecutionException, ParseException {
        this.node = node;
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


    private Histogram extractHistogram(BinaryJoinExecutionNode.JoinOperand operand) throws ExecutorEngine.LocalQueryExecutionException {
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
        String query = "SELECT * INTO %s FROM %s WHERE width_bucket(%s, %s, %s, %s) IN %s;";
        String desiredBuckets = "(" + bucketsToInclude.stream().map(i -> i.toString()).collect(Collectors.joining(", ")) + ")";
        return String.format(query, dest, operand.table, operand.attribute, min, max, NUM_BUCKETS, desiredBuckets);
    }

    private QueryExecutionPlan createQueryExecutionPlan(ShuffleJoinPartitionAssignments assignments) {
        // TODO: get the proper island instead of assuming relational
        QueryExecutionPlan plan = new QueryExecutionPlan(IslandsAndCast.Scope.RELATIONAL);

        String sendToRightDestination = this.node.getTableName().get() + "_LEFTPARTIAL";
        String sendToRightQuery = getPartitionQuery(node.getLeft(), assignments.getBucketsForJoinOperand(node.getRight()), sendToRightDestination);
        LocalQueryExecutionNode sendToRight = new LocalQueryExecutionNode(sendToRightQuery, node.getLeft().engine, sendToRightDestination);

        String sendToLeftDestination = this.node.getTableName().get() + "_RIGHTPARTIAL";
        String sendToLeftQuery = getPartitionQuery(node.getLeft(), assignments.getBucketsForJoinOperand(node.getLeft()), sendToLeftDestination);
        LocalQueryExecutionNode sendToLeft = new LocalQueryExecutionNode(sendToLeftQuery, node.getLeft().engine, sendToLeftDestination);

        String rightResultDestination = this.node.getTableName().get() + "_RIGHTRESULTS";
        String rightQueryString = node.getRight().getQueryString();
        LocalQueryExecutionNode rightResults = new LocalQueryExecutionNode(rightQueryString, node.getRight().engine, rightResultDestination);

        String leftResultDestination = this.node.getTableName().get() + "_LEFTRESULTS";
        String leftQueryString = node.getLeft().getQueryString();
        LocalQueryExecutionNode leftResults = new LocalQueryExecutionNode(leftQueryString, node.getLeft().engine, leftResultDestination);

        LocalQueryExecutionNode union = new LocalQueryExecutionNode(node.getShuffleUnionString(leftResultDestination, rightResultDestination), node.getEngine(), node.getTableName().get());

        plan.addNode(sendToRight);
        plan.addNode(sendToLeft);
        plan.addNode(rightResults);
        plan.addNode(leftResults);

        plan.addNode(union);
        plan.setTerminalTableName(union.getTableName().get());
        plan.setTerminalTableNode(union);

        plan.addEdge(sendToRight, rightResults);
        plan.addEdge(sendToLeft, leftResults);
        plan.addEdge(leftResults, union);
        plan.addEdge(rightResults, union);

        return plan;
    }

    public Optional<QueryResult> execute() throws ExecutorEngine.LocalQueryExecutionException, MigrationException {

        Histogram leftDistribution = extractHistogram(node.getLeft());
        Histogram rightDistribution = extractHistogram(node.getRight());

        ShuffleJoinPartitionAssignments assignments = this.assignBuckets(leftDistribution, rightDistribution);

        return Optional.ofNullable(Executor.executePlan(this.createQueryExecutionPlan(assignments)));
    }
}

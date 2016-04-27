package istc.bigdawg.executor.shuffle;

import com.diffplug.common.base.Errors;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.executor.plan.LocalQueryExecutionNode;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.utils.IslandsAndCast;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ShuffleJoinExecutor {
    private static final int NUM_BUCKETS = 100;

    private BinaryJoinExecutionNode node;

    public ShuffleJoinExecutor(BinaryJoinExecutionNode node) throws ExecutorEngine.LocalQueryExecutionException, ParseException {
        this.node = node;
    }

    private Collection<Histogram> extractHistograms(Collection<BinaryJoinExecutionNode.JoinOperand> operands) throws ExecutorEngine.LocalQueryExecutionException {
        Stream<Pair<Double, Double>> minMaxStream =  operands.stream()
                .map(Errors.rethrow().wrap((BinaryJoinExecutionNode.JoinOperand o) -> o.engine.getMinMax(o.table, o.attribute)))
                .map(p -> new ImmutablePair<>(p.getLeft().doubleValue(), p.getRight().doubleValue()));

        double min = minMaxStream.mapToDouble(Pair::getLeft).min().getAsDouble();
        double max = minMaxStream.mapToDouble(Pair::getRight).max().getAsDouble();

        return operands.stream()
                .map(Errors.rethrow().wrap((BinaryJoinExecutionNode.JoinOperand o) -> new Histogram(o.engine.computeHistogram(o.table, o.attribute, min, max, NUM_BUCKETS), min, max, o)))
                .collect(Collectors.toSet());
    }

    private String getPartitionQuery(BinaryJoinExecutionNode.JoinOperand operand, Collection<Integer> bucketsToInclude, String dest, double min, double max) {
        String query = "SELECT * INTO %s FROM %s WHERE width_bucket(%s, %s, %s, %s) IN %s;";
        String desiredBuckets = "(" + bucketsToInclude.stream().map(i -> i.toString()).collect(Collectors.joining(", ")) + ")";
        return String.format(query, dest, operand.table, operand.attribute, min, max, NUM_BUCKETS, desiredBuckets);
    }

    private QueryExecutionPlan createQueryExecutionPlan(Assignments assignments) {
        double min = assignments.getMinValue();
        double max = assignments.getMaxValue();

        // TODO: get the proper island instead of assuming relational
        QueryExecutionPlan plan = new QueryExecutionPlan(IslandsAndCast.Scope.RELATIONAL);

        String sendToRightDestination = this.node.getTableName().get() + "_LEFTPARTIAL";
        String sendToRightQuery = getPartitionQuery(node.getLeft(), assignments.getBucketsForJoinOperand(node.getRight()), sendToRightDestination, min, max);
        LocalQueryExecutionNode sendToRight = new LocalQueryExecutionNode(sendToRightQuery, node.getLeft().engine, sendToRightDestination);

        String sendToLeftDestination = this.node.getTableName().get() + "_RIGHTPARTIAL";
        String sendToLeftQuery = getPartitionQuery(node.getLeft(), assignments.getBucketsForJoinOperand(node.getLeft()), sendToLeftDestination, min, max);
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
        Collection<Histogram> histograms = this.extractHistograms(this.node.getOperands());
        Assignments assignments = Assignments.minimizeBandwidth(histograms, NUM_BUCKETS);
        QueryExecutionPlan shufflePlan = this.createQueryExecutionPlan(assignments);

        return Optional.ofNullable(Executor.executePlan(shufflePlan));
    }
}

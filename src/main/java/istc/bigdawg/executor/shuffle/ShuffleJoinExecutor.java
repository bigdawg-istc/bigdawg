package istc.bigdawg.executor.shuffle;

import com.jcabi.log.Logger;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.executor.plan.ExecutionNode;
import istc.bigdawg.executor.plan.LocalQueryExecutionNode;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.IslandsAndCast;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

public class ShuffleJoinExecutor {

    private BinaryJoinExecutionNode node;

    public ShuffleJoinExecutor(BinaryJoinExecutionNode node) throws ExecutorEngine.LocalQueryExecutionException, ParseException {
        this.node = node;
    }

    private String getPartitionQuery(BinaryJoinExecutionNode.JoinOperand operand, String destinationTable, Collection<Assignments.Range> desiredRanges, Collection<Assignments.Hotspot> desiredHotspots, Collection<Assignments.Hotspot> ignore) {
        String ranges = desiredRanges.stream().map(r -> String.format("(%s BETWEEN %s and %s)", operand.attribute, r.start, r.end)).collect(Collectors.joining(" OR "));
        String hotspots = String.format("%s IN (%s)", operand.attribute, desiredHotspots.stream().map(h -> Long.toString(h.val)).collect(Collectors.joining(", ")));
        String blacklist = String.format("%s IN (%s)", operand.attribute, ignore.stream().map(h -> Long.toString(h.val)).collect(Collectors.joining(", ")));

        return String.format("SELECT * INTO %s FROM %s WHERE (%s OR %s) AND NOT %s;", destinationTable, operand.table, ranges, hotspots, blacklist);
    }

    private QueryExecutionPlan createQueryExecutionPlan(Assignments assignments) {
        // TODO: get the proper island instead of assuming relational
        QueryExecutionPlan plan = new QueryExecutionPlan(IslandsAndCast.Scope.RELATIONAL);

        String sendToRightDestination = this.node.getTableName().get() + "_LEFTPARTIAL";
        String sendToRightQuery = getPartitionQuery(node.getLeft(), sendToRightDestination, assignments.getRangesForJoinOperand(node.getRight()),
                assignments.getHotspotsForJoinOperand(node.getRight()), assignments.getHotspotsNotForJoinOperand(node.getRight()));
        LocalQueryExecutionNode sendToRight = new LocalQueryExecutionNode(sendToRightQuery, node.getLeft().engine, sendToRightDestination);

        String sendToLeftDestination = this.node.getTableName().get() + "_RIGHTPARTIAL";
        String sendToLeftQuery = getPartitionQuery(node.getRight(), sendToLeftDestination, assignments.getRangesForJoinOperand(node.getLeft()),
                assignments.getHotspotsForJoinOperand(node.getLeft()), assignments.getHotspotsNotForJoinOperand(node.getLeft()));
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

    private QueryExecutionPlan generateQEP(Assignments assignments) throws DirectedAcyclicGraph.CycleFoundException {
        // TODO: implement this in the future for non-binary joins!
        QueryExecutionPlan plan = new QueryExecutionPlan(IslandsAndCast.Scope.RELATIONAL);

        Map<ConnectionInfo, ExecutionNode> localJoins = new HashMap<>();

        for (BinaryJoinExecutionNode.JoinOperand o : this.node.getOperands()) {
            String query = null; //TODO
            String destination = null; //TODO
            LocalQueryExecutionNode localJoin = new LocalQueryExecutionNode(query, o.engine, destination);
            plan.addNode(localJoin);
            localJoins.put(o.engine, localJoin);

            for (BinaryJoinExecutionNode.JoinOperand other : this.node.getOperands()) {
                if (!other.equals(o)) {
                    String partitionDestination = null; //TODO
                    String partitionQuery = null; //TODO

                    LocalQueryExecutionNode partitionOther = new LocalQueryExecutionNode(partitionQuery, other.engine, partitionDestination);
                    plan.addNode(partitionOther);
                    plan.addDependencies(localJoin, Collections.singleton(partitionOther));
                }
            }
        }

        String terminalQuery = null; // TODO
        ExecutionNode terminal = new LocalQueryExecutionNode(terminalQuery, node.getEngine(), node.getTableName().get());

        plan.addNode(terminal);
        plan.addDependencies(terminal, localJoins.values());
        plan.setTerminalTableNode(terminal);
        plan.setTerminalTableName(terminal.getTableName().get());

        return plan;
    }

    public Optional<QueryResult> execute() throws ExecutorEngine.LocalQueryExecutionException, MigrationException, ConnectionInfo.LocalQueryExecutorLookupException {
        Logger.info(this, "Extracting histogram from engines...");
        // TODO: get histogram strat from node
        Collection<Histogram> histograms = ShuffleEngine.createHistograms(this.node.getOperands(), ShuffleEngine.HistogramStrategy.SAMPLING);

        Logger.info(this, "Creating assignment from engines...");
        //TODO: get assignment strat from node
        Assignments assignments = Assignments.assignTuples(histograms, this.node.getEngine(), ShuffleEngine.NUM_BUCKETS, Assignments.AssignmentStrategies.MINIMUM_BANDWIDTH);

        Logger.info(this, "Creating QEP from assignments...");
        QueryExecutionPlan shufflePlan = this.createQueryExecutionPlan(assignments);

        return Optional.ofNullable(Executor.executePlan(shufflePlan));
    }
}

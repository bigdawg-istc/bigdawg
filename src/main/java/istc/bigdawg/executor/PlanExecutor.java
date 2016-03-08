package istc.bigdawg.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.migration.MigrationResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.ExecutionNode;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.migration.Migrator;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import istc.bigdawg.query.ConnectionInfo;

/**
 * TODO:
 *   fully abstracted DbHandlers instead of casting to PostgreSQL
 *   shuffle joins
 *   better exception/error handling in the event of failure
 *   look into implications of too many events in the ForkJoinPool
 * 
 * @author ankush
 */
class PlanExecutor {
    static final Logger log = Logger.getLogger(PlanExecutor.class.getName());
    static final Monitor monitor = new Monitor();

    private final Multimap<ExecutionNode, ConnectionInfo> resultLocations = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    private final Multimap<ConnectionInfo, String> temporaryTables = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private final Map<ImmutablePair<String, ConnectionInfo>, CompletableFuture<MigrationResult>> migrations = new ConcurrentHashMap<>();

    private final Map<ExecutionNode, CountDownLatch> locks = new ConcurrentHashMap<>();

    private final QueryExecutionPlan plan;

    /**
     * Class responsible for handling the execution of a single QueryExecutionPlan
     *
     * @param plan
     *            a data structure of the queries to be run and their ordering,
     *            with edges pointing to dependencies
     */
    public PlanExecutor(QueryExecutionPlan plan) {
        this.plan = plan;

        // initialize countdown latches to the proper counts
        for(ExecutionNode node : plan) {
            locks.put(node, new CountDownLatch(plan.getDependencies(node).size()));
        }
    }

    /**
     * Execute the plan, and return the result
     */
    public Optional<QueryResult> executePlan() throws SQLException, MigrationException {
        long start = System.currentTimeMillis();

        log.debug(String.format("Executing query plan %s...", plan.getSerializedName()));

        CompletableFuture<Optional<QueryResult>> finalResult = CompletableFuture.completedFuture(Optional.empty());

        for (ExecutionNode node : plan) {
            log.debug(String.format("Examining query node %s...", node.getTableName()));

            CompletableFuture<Optional<QueryResult>> result = CompletableFuture.supplyAsync(() -> this.executeNode(node));

            if (plan.getTerminalTableNode().equals(node)) {
                finalResult = result;
            }
        }

        // Block until finalResult has resolved
        Optional<QueryResult> result = Optional.empty();
        try {
            result = finalResult.get();
        } catch (InterruptedException e) {
            log.error(String.format("Execution of query plan %s was interrupted", plan.getSerializedName()), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error(String.format("Error retrieving results of final query node %s", plan.getSerializedName()), e);
        }

        dropTemporaryTables();

        long end = System.currentTimeMillis();

        log.debug(String.format("Finished executing query plan %s, in %d seconds.", plan.getSerializedName(), (start - end) / 1000));
        log.debug(String.format("Sending timing to monitor..."));
        monitor.finishedBenchmark(plan, start, end);

        log.debug(String.format("Returning result to planner..."));
        return result;
    }
    
    private Optional<QueryResult> executeNode(ExecutionNode node) {
        // perform shuffle join if equijoin and hint doesn't specify otherwise
        if (node instanceof BinaryJoinExecutionNode &&
                ((BinaryJoinExecutionNode) node).getHint().orElse(BinaryJoinExecutionNode.JoinAlgorithms.SHUFFLE) == BinaryJoinExecutionNode.JoinAlgorithms.SHUFFLE &&
                ((BinaryJoinExecutionNode) node).isEquiJoin()) {
            BinaryJoinExecutionNode joinNode = (BinaryJoinExecutionNode) node;
            if(!joinNode.getHint().isPresent() || joinNode.getHint().get() == BinaryJoinExecutionNode.JoinAlgorithms.SHUFFLE) {
                try {
                    // TODO(ankush): colocate non-operand dependencies!
                    Optional<QueryResult> result = new ShuffleJoinExecutor(joinNode).execute();
                    markNodeAsCompleted(node);
                    return result;
                } catch (Exception e) {
                    log.error(String.format("Error executing node %s", joinNode), e);
                    return Optional.empty();
                }
            }
        }
        // otherwise execute as local query execution (same as broadcast join)

        // colocate dependencies, blocking until completed
        colocateDependencies(node);
        log.debug(String.format("Executing query node %s...", node.getTableName()));

        return node.getQueryString().flatMap((query) -> {
            try {
                Optional<QueryResult> result = ((PostgreSQLHandler) node.getEngine().getHandler()).executePostgreSQL(query);
                markNodeAsCompleted(node);
                return result;
            } catch (SQLException e) {
                log.error(String.format("Error executing node %s", node), e);
                return Optional.empty();
            }
        });
    }

    private void markNodeAsCompleted(ExecutionNode node) {
        if (!plan.getTerminalTableNode().equals(node)) {
            // clean up the intermediate table later
            node.getTableName().ifPresent((table) -> temporaryTables.put(node.getEngine(), table));

            // update nodeLocations to reflect that the results are located on this node's engine
            resultLocations.put(node, node.getEngine());

            for (ExecutionNode dependent : plan.getDependents(node)) {
                locks.get(dependent).countDown();
            }
        }
    }


    /**
     * Colocates the dependencies for the given ExecutionNode onto that node's engine.
     *
     * Waits for any incomplete dependencies, and blocks the current thread until completion.
     *
     * @param node
     */
    private void colocateDependencies(ExecutionNode node) {
        // Block until dependencies are all resolved
        try {
            log.debug(String.format("Waiting for dependencies of query node %s to be resolved", node));
            this.locks.get(node).await();
        } catch (InterruptedException e) {
            log.error(String.format("Execution of query node %s was interrupted while waiting for dependencies.", node), e);
            Thread.currentThread().interrupt();
        }

        log.debug(String.format("Colocating dependencies of query node %s", node));

        CompletableFuture[] futures = plan.getDependencies(node).stream()
                .filter(d -> !resultLocations.containsEntry(d, node.getEngine()) && d.getTableName().isPresent())
                .map((d) -> {
                    // computeIfAbsent gets a previous migration's Future, or creates one if it doesn't already exist
                    return migrations.computeIfAbsent(new ImmutablePair<>(d.getTableName().get(), node.getEngine()), (k) -> {
                        return CompletableFuture.supplyAsync(() -> colocateSingleDependency(d, node));
                    });
                }).toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private MigrationResult colocateSingleDependency(ExecutionNode dependency, ExecutionNode dependant) {
        return dependency.getTableName().map((table) -> {
            log.debug(String.format("Migrating dependency table %s from engine %s to engine %s...", table, dependency.getEngine(),
                    dependant.getEngine()));
            try {
                MigrationResult result = Migrator.migrate(dependency.getEngine(), table, dependant.getEngine(), table);

                // mark the dependency's data as being present on node.getEngine()
                resultLocations.put(dependency, dependant.getEngine());

                // mark that this engine now has a copy of the dependency's data
                temporaryTables.put(dependant.getEngine(), table);

                return result;
            } catch (MigrationException e) {
                log.error(String.format("Error migrating dependency %s of node %s!", dependency, dependant), e);
                return MigrationResult.getFailedInstance(e.getLocalizedMessage());
            }
        }).orElse(MigrationResult.getEmptyInstance("No table to migrate"));
    }

    private void dropTemporaryTables() throws SQLException {
        synchronized(temporaryTables) {
            Multimap<ConnectionInfo, String> removed = HashMultimap.create();

            for (ConnectionInfo c : temporaryTables.keySet()) {
                Collection<String> tables = temporaryTables.get(c);

                log.debug(String.format("Cleaning up %s by removing %s...", c, tables));
                ((PostgreSQLHandler) c.getHandler()).executeStatementPostgreSQL(c.getCleanupQuery(tables));

                removed.putAll(c, tables);
            }

            for (Map.Entry<ConnectionInfo, String> entry : removed.entries()) {
                temporaryTables.remove(entry.getKey(), entry.getValue());
            }
        }

        log.debug(String.format("Temporary tables for query plan %s have been removed", plan));
    }


    /**
     * Colocates the dependencies for the given ExecutionNode onto that node's engine one at a time.
     *
     * Waits for any incomplete dependencies, and blocks the current thread until completion.
     *
     * @param node
     */
    @Deprecated
    private void colocateDependenciesSerially(ExecutionNode node) {
        // Block until dependencies are all resolved
        try {
            log.debug(String.format("Waiting for dependencies of query node %s to be resolved", node));
            this.locks.get(node).await();
        } catch (InterruptedException e) {
            log.error(String.format("Execution of query node %s was interrupted while waiting for dependencies.", node), e);
            Thread.currentThread().interrupt();
        }

        log.debug(String.format("Colocating dependencies of query node %s", node));

        plan.getDependencies(node).stream()
                // only look at dependencies not already on desired engine
                .filter(d -> !resultLocations.containsEntry(d, node.getEngine()))
                .forEach(d -> {
                    // migrate to node.getEngine()
                    d.getTableName().ifPresent((table) -> {
                        log.debug(String.format("Migrating dependency table %s from engine %s to engine %s...", table, d.getEngine(),
                                node.getEngine()));
                        try {
                            Migrator.migrate(d.getEngine(), table, node.getEngine(), table);
                            // mark the dependency's data as being present on node.getEngine()
                            resultLocations.put(d, node.getEngine());

                            // mark that this engine now has a copy of the dependency's data
                            temporaryTables.put(node.getEngine(), table);
                        } catch (MigrationException e) {
                            log.error(String.format("Error migrating dependency %s of node %s!", d, node), e);
                        }
                    });
                });
    }
}

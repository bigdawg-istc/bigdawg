package istc.bigdawg.executor;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import istc.bigdawg.query.ConnectionInfo;

import java.util.Spliterators;

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

        StringBuilder sb = new StringBuilder();
        for(ExecutionNode n : plan) {
            sb.append(String.format("%s -> (%s)\n", n, plan.getDependents(n)));
        }

        log.debug(String.format("Received plan %s: \n %s", plan.getSerializedName(), sb.toString()));

        log.debug(String.format("Ordered queries: \n %s",
                StreamSupport.stream(Spliterators.spliterator(plan.iterator(), plan.vertexSet().size(), Spliterator.ORDERED), false)
                    .map(ExecutionNode::getQueryString)
                    .filter(Optional::isPresent).map(opt -> opt.get())
                    .collect(Collectors.joining(" \n ---- then ---- \n "))));

        // initialize countdown latches to the proper counts
        for(ExecutionNode node : plan) {
            int latchSize = plan.inDegreeOf(node);
            log.debug(String.format("Node %s lock initialized to %s", node.getTableName(), latchSize));
            locks.put(node, new CountDownLatch(latchSize));
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
        // TODO(ankush): re-enable this and debug
//        if (node instanceof BinaryJoinExecutionNode &&
//                ((BinaryJoinExecutionNode) node).getHint().orElse(BinaryJoinExecutionNode.JoinAlgorithms.SHUFFLE) == BinaryJoinExecutionNode.JoinAlgorithms.SHUFFLE &&
//                ((BinaryJoinExecutionNode) node).isEquiJoin()) {
//            BinaryJoinExecutionNode joinNode = (BinaryJoinExecutionNode) node;
//            if(!joinNode.getHint().isPresent() || joinNode.getHint().get() == BinaryJoinExecutionNode.JoinAlgorithms.SHUFFLE) {
//                try {
//                    colocateDependencies(node, Arrays.asList(joinNode.getLeft().table, joinNode.getRight().table));
//
//                    Optional<QueryResult> result = new ShuffleJoinExecutor(joinNode).execute();
//                    markNodeAsCompleted(node);
//                    return result;
//                } catch (Exception e) {
//                    log.error(String.format("Error executing node %s", joinNode), e);
//                    return Optional.empty();
//                }
//            }
//        }


        // otherwise execute as local query execution (same as broadcast join)
        // colocate dependencies, blocking until completed
        colocateDependencies(node, Collections.emptySet());
        log.debug(String.format("Executing query node %s...", node.getTableName()));

        return node.getQueryString().flatMap((query) -> {
            try {
                Optional<QueryResult> result = ((PostgreSQLHandler) node.getEngine().getHandler()).executePostgreSQL(query);
                return result;
            } catch (SQLException e) {
                log.error(String.format("Error executing node %s", node.getTableName()), e);
                // TODO: if error is actually bad, don't markNodeAsCompleted, and instead fail the QEP gracefully.
                return Optional.empty();
            } finally {
                markNodeAsCompleted(node);
            }
        });
    }

    private void markNodeAsCompleted(ExecutionNode node) {
        log.debug(String.format("Marking node %s as completed.", node.getTableName()));

        if (!plan.getTerminalTableNode().equals(node)) {
            // clean up the intermediate table later
            node.getTableName().ifPresent((table) -> temporaryTables.put(node.getEngine(), table));

            // update nodeLocations to reflect that the results are located on this node's engine
            resultLocations.put(node, node.getEngine());

            for (ExecutionNode dependent : plan.getDependents(node)) {
                log.debug(String.format("Decrementing lock of dependency %s", dependent.getTableName()));
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
    private void colocateDependencies(ExecutionNode node, Collection<String> ignoreTables) {
        // Block until dependencies are all resolved
        try {
            log.debug(String.format("Waiting for dependencies of query node %s to be resolved...", node.getTableName()));
            this.locks.get(node).await();
        } catch (InterruptedException e) {
            log.error(String.format("Execution of query node %s was interrupted while waiting for dependencies.", node.getTableName()), e);
            Thread.currentThread().interrupt();
        }

        log.debug(String.format("Colocating dependencies of query node %s", node.getEngine()));

        CompletableFuture[] futures = plan.getDependencies(node).stream()
                .filter(d -> !resultLocations.containsEntry(d, node.getEngine()) && d.getTableName().isPresent() && !ignoreTables.contains(d.getTableName().get()))
                .map((d) -> {
                    // computeIfAbsent gets a previous migration's Future, or creates one if it doesn't already exist
                    ImmutablePair<String, ConnectionInfo> migrationKey = new ImmutablePair<>(d.getTableName().get(), node.getEngine());

                    return migrations.computeIfAbsent(migrationKey, (k) -> {
                        return CompletableFuture.supplyAsync(() -> colocateSingleDependency(d, node));
                    });
                }).toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private MigrationResult colocateSingleDependency(ExecutionNode dependency, ExecutionNode dependant) {
        return dependency.getTableName().map((table) -> {
            log.debug(String.format("Migrating dependency table %s from engine %s to engine %s...", table, dependency.getEngine().getDatabase(),
                    dependant.getEngine().getDatabase()));
            try {
                MigrationResult result = Migrator.migrate(dependency.getEngine(), table, dependant.getEngine(), table);

                // mark the dependency's data as being present on node.getEngine()
                resultLocations.put(dependency  , dependant.getEngine());

                // mark that this engine now has a copy of the dependency's data
                temporaryTables.put(dependant.getEngine(), table);

                log.debug(String.format("Finished migrating dependency %s of node %s!", dependency.getTableName(), dependant.getTableName()));
                return result;
            } catch (MigrationException e) {
                log.error(String.format("Error migrating dependency %s of node %s!", dependency.getTableName(), dependant.getTableName()), e);
                return MigrationResult.getFailedInstance(e.getLocalizedMessage());
            }
        }).orElse(MigrationResult.getEmptyInstance(String.format("No table to migrate for node %s", dependency.getTableName())));
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

        log.debug(String.format("Temporary tables for query plan %s have been removed", plan.getSerializedName()));
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
            log.debug(String.format("Waiting for dependencies of query node %s to be resolved", node.getTableName()));
            this.locks.get(node).await();
        } catch (InterruptedException e) {
            log.error(String.format("Execution of query node %s was interrupted while waiting for dependencies.", node.getTableName()), e);
            Thread.currentThread().interrupt();
        }

        log.debug(String.format("Colocating dependencies of query node %s", node.getTableName()));

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
                            log.error(String.format("Error migrating dependency %s of node %s!", d.getTableName(), node.getTableName()), e);
                        }
                    });
                });
    }
}

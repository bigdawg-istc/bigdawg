package istc.bigdawg.executor;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class Executor {
    public static QueryResult executePlan(QueryExecutionPlan plan) throws SQLException, MigrationException {
        return new PlanExecutor(plan).executePlan().orElse(null);
    }

    public static CompletableFuture<Optional<QueryResult>> executePlanAsync(QueryExecutionPlan plan) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new PlanExecutor(plan).executePlan();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }
}

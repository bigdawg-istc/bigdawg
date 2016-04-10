package istc.bigdawg.executor;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.jcabi.log.Logger;
import istc.bigdawg.signature.Signature;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class Executor {
    public static QueryResult executePlan(QueryExecutionPlan plan, Signature sig, int index) throws SQLException, MigrationException {
        return new PlanExecutor(plan).executePlan(Optional.of(new ImmutablePair<>(sig, index))).orElse(null);
    }

    public static QueryResult executePlan(QueryExecutionPlan plan) throws SQLException, MigrationException {
        return new PlanExecutor(plan).executePlan(Optional.empty()).orElse(null);
    }

    public static CompletableFuture<Optional<QueryResult>> executePlanAsync(QueryExecutionPlan plan, Optional<Pair<Signature, Integer>> reportValues) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new PlanExecutor(plan).executePlan(reportValues);
            } catch (Exception e) {
                Logger.error(Executor.class, "Error executing query plan: %[exception]s", e);
                throw new CompletionException(e);
            }
        });
    }
}

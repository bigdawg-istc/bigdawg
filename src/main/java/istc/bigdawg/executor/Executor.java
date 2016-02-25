package istc.bigdawg.executor;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;


import java.sql.SQLException;

public class Executor {
    public static QueryResult executePlan(QueryExecutionPlan plan) throws SQLException, MigrationException {
        return new PlanExecutor(plan).executePlan();
    }
}

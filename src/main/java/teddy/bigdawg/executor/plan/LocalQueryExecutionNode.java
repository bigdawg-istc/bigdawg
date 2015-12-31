package teddy.bigdawg.executor.plan;

import java.util.Optional;

import istc.bigdawg.query.ConnectionInfo;

/**
 * Represents a query execution on a single engine into a destination table on
 * the same engine
 * 
 * @author ankushg
 */
public class LocalQueryExecutionNode implements ExecutionNode {
    private final String query;
    private final ConnectionInfo engine;
    private final String resultsTable;

    /**
     * Create a LocalQueryExecutionNode representing a query execution on a
     * single engine into a destination table on the same engine
     * 
     * @param query
     *            the query to be evaluated
     * @param engine
     *            the database engine where the query is to be executed
     * @param resultsTable
     *            the name of the table on the specified database engine where
     *            the query results will be stored (must be unique across all
     *            engines)
     */
    public LocalQueryExecutionNode(String query, ConnectionInfo engine, String resultsTable) {
        this.query = query;
        this.engine = engine;
        this.resultsTable = resultsTable;
    }

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getEngineId()
     */
    @Override
    public ConnectionInfo getEngine() {
        return this.engine;
    }

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getTableName()
     */
    @Override
    public Optional<String> getTableName() {
        return Optional.of(this.resultsTable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getQueryString()
     */
    @Override
    public Optional<String> getQueryString() {
        return Optional.of(this.query);
    }

}

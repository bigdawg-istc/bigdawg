package teddy.bigdawg.executor.plan;

import java.util.Optional;

/**
 * Represents a query execution on a single engine into a destination table on
 * the same engine
 * 
 * @author ankushg
 */
public class LocalQueryExecutionNode implements ExecutionNode {
    private final String query;
    private final int engineId;
    private final String resultsTable;

    /**
     * Create a LocalQueryExecutionNode representing a query execution on a
     * single engine into a destination table on the same engine
     * 
     * @param query
     *            the query to be evaluated
     * @param engineId
     *            the database engine where the query is to be executed
     * @param resultsTable
     *            the name of the table on the specified database engine where
     *            the query results will be stored (must be unique across all
     *            engines)
     */
    public LocalQueryExecutionNode(String query, int engineId, String resultsTable) {
        this.query = query;
        this.engineId = engineId;
        this.resultsTable = resultsTable;
    }

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getEngineId()
     */
    @Override
    public int getEngineId() {
        return this.engineId;
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

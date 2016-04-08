package istc.bigdawg.executor.plan;

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
     * @see istc.bigdawg.executor.plan.ExecutionNode#getEngineId()
     */
    @Override
    public ConnectionInfo getEngine() {
        return this.engine;
    }

    /*
     * (non-Javadoc)
     * 
     * @see istc.bigdawg.executor.plan.ExecutionNode#getTableName()
     */
    @Override
    public Optional<String> getTableName() {
        return Optional.of(this.resultsTable);
    }

    /*
     * (non-Javadoc)
     * 
     * @see istc.bigdawg.executor.plan.ExecutionNode#getQueryString()
     */
    @Override
    public Optional<String> getQueryString() {
        return Optional.of(this.query);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((engine == null) ? 0 : engine.hashCode());
        result = prime * result + ((query == null) ? 0 : query.hashCode());
        result = prime * result + ((resultsTable == null) ? 0 : resultsTable.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof LocalQueryExecutionNode))
            return false;
        LocalQueryExecutionNode other = (LocalQueryExecutionNode) obj;
        if (engine == null) {
            if (other.engine != null)
                return false;
        } else if (!engine.equals(other.engine))
            return false;
        if (query == null) {
            if (other.query != null)
                return false;
        } else if (!query.equals(other.query))
            return false;
        if (resultsTable == null) {
            if (other.resultsTable != null)
                return false;
        } else if (!resultsTable.equals(other.resultsTable))
            return false;
        return true;
    }

    public String serialize() {
        return "LocalQueryExecutionNode [query=" + query + ", engine=" + engine + ", resultsTable=" + resultsTable + "]";
    }

    @Override
    public String toString() {
        return resultsTable;
    }
}

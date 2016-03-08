package istc.bigdawg.executor.plan;

import java.util.Optional;

import istc.bigdawg.query.ConnectionInfo;

/**
 * Represents a single table on a given engine.
 * 
 * @author ankushg
 */
public class TableExecutionNode implements ExecutionNode {
    private final ConnectionInfo engine;
    private final String tableName;

    /**
     * Create an ExecutionNode representing a single table on a given engine.
     * 
     * @param engine
     *            the database engine where the table is located
     * @param tableName
     *            the name of the table on the specified database engine (must
     *            be unique across all engines)
     */
    public TableExecutionNode(ConnectionInfo engine, String tableName) {
        this.engine = engine;
        this.tableName = tableName;
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
        return Optional.of(this.tableName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see istc.bigdawg.executor.plan.ExecutionNode#getQueryString()
     */
    @Override
    public Optional<String> getQueryString() {
        return Optional.empty();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((engine == null) ? 0 : engine.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableExecutionNode other = (TableExecutionNode) obj;
        if (engine == null) {
            if (other.engine != null)
                return false;
        } else if (!engine.equals(other.engine))
            return false;
        if (tableName == null) {
            if (other.tableName != null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TableExecutionNode [engine=" + engine + ", tableName=" + tableName + "]";
    }
}
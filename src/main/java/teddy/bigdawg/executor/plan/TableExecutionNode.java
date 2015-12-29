package teddy.bigdawg.executor.plan;

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
     * @param engineId
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
        return Optional.of(this.tableName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getQueryString()
     */
    @Override
    public Optional<String> getQueryString() {
        return Optional.empty();
    }
}
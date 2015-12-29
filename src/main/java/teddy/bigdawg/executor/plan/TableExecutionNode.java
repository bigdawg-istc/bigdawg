package teddy.bigdawg.executor.plan;

import java.util.Optional;

/**
 * Represents a single table on a given engine.
 * 
 * @author ankushg
 */
public class TableExecutionNode implements ExecutionNode {
    private final int engineId;
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
    public TableExecutionNode(int engineId, String tableName) {
        this.engineId = engineId;
        this.tableName = tableName;
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
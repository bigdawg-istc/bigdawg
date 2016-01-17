package istc.bigdawg.executor.plan;

import java.util.Optional;

import istc.bigdawg.query.ConnectionInfo;

/**
 * Represents a task to be completed by the Executor in order to execute a
 * query.
 * 
 * @author ankushg
 */
public interface ExecutionNode {

    /**
     * @return ConnectionInfo representing the database engine where the node should be
     *         executed
     */
    public ConnectionInfo getEngine();

    /**
     * @return table name for where the result will be stored. If empty, then
     *         the result will be returned via the callback.
     */
    public Optional<String> getTableName();

    /**
     * @return string to be used to query the engine represented by
     *         getEngineId(). If empty, then no query needs to be executed for
     *         the given node.
     */
    public Optional<String> getQueryString();
}
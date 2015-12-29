package teddy.bigdawg.executor.plan;

import java.util.Optional;

import istc.bigdawg.query.ConnectionInfo;

/**
 * Represents a cross-engine Join.
 * 
 * Not necessary for 1/5/16 milestone, as naive cross-engine joins within
 * relational island can be represented as a LocalExecutionQueryNode after
 * migrating dependencies to the proper engine
 * 
 * @author ankushg
 *
 */
public class BinaryJoinExecutionNode implements ExecutionNode {
    // TODO(ankush) Implement BinaryJoinExecutionNode

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getEngineId()
     */
    @Override
    public ConnectionInfo getEngine() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getTableName()
     */
    @Override
    public Optional<String> getTableName() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see teddy.bigdawg.executor.plan.ExecutionNode#getQueryString()
     */
    @Override
    public Optional<String> getQueryString() {
        throw new UnsupportedOperationException();
    }

}

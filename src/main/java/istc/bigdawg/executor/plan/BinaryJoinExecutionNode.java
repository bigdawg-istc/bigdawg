package istc.bigdawg.executor.plan;

import java.text.MessageFormat;
import java.util.Optional;

import com.google.common.base.Objects;
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

    public enum JoinAlgorithms {
        BROADCAST,
        SHUFFLE
    }

    /**
     * Contains information that the Executor needs in order to make decisions about how to best perform the binary join
     */
    public class JoinOperand {
        public final ConnectionInfo engine;
        public final String table;
        public final String attribute;

        /**
         * Query if the join were to be executed on this engine with parameters {0} = other operand and {1} = destination table
         *
         * Example: "SELECT * FROM table JOIN {0} ON table.attrib = {0}.attrib INTO {1}"
         */
        public final String joinTemplate;

        public JoinOperand(ConnectionInfo engine, String table, String attribute, String joinTemplate) {
            this.engine = engine;
            this.table = table;
            this.attribute = attribute;
            this.joinTemplate = joinTemplate;
        }

        public String getQueryString(String inputTable, String outputTable) {
            return MessageFormat.format(joinTemplate, inputTable, outputTable);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JoinOperand that = (JoinOperand) o;
            return Objects.equal(engine, that.engine) &&
                    Objects.equal(table, that.table) &&
                    Objects.equal(attribute, that.attribute);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(engine, table, attribute);
        }
    }

    final JoinOperand left;
    final JoinOperand right;
    final Optional<JoinAlgorithms> hint;
    final ConnectionInfo destinationEngine;
    final String destinationTable;
    final String broadcastQuery;

    public BinaryJoinExecutionNode(String broadcastQuery, ConnectionInfo destinationEngine, String destinationTable, JoinOperand left, JoinOperand right, Optional<JoinAlgorithms> hint) {
        this.broadcastQuery = broadcastQuery;
        this.destinationEngine = destinationEngine;
        this.destinationTable = destinationTable;
        this.left = left;
        this.right = right;
        this.hint = hint;
    }

    public BinaryJoinExecutionNode(String broadcastQuery, ConnectionInfo destinationEngine, String destinationTable, JoinOperand left, JoinOperand right) {
        this(broadcastQuery, destinationEngine, destinationTable, left, right, Optional.empty());
    }

    public BinaryJoinExecutionNode(String broadcastQuery, ConnectionInfo destinationEngine, String destinationTable, JoinOperand left, JoinOperand right, JoinAlgorithms hint) {
        this(broadcastQuery, destinationEngine, destinationTable, left, right, Optional.of(hint));
    }

    public JoinOperand getLeft(){
        return this.left;
    }

    public JoinOperand getRight() {
        return this.right;
    }

    public Optional<JoinAlgorithms> getHint() {
        return this.hint;
    }

    /*
     * (non-Javadoc)
     * 
     * @see istc.bigdawg.executor.plan.ExecutionNode#getEngineId()
     */
    @Override
    public ConnectionInfo getEngine() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see istc.bigdawg.executor.plan.ExecutionNode#getTableName()
     */
    @Override
    public Optional<String> getTableName() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see istc.bigdawg.executor.plan.ExecutionNode#getQueryString()
     */
    @Override
    public Optional<String> getQueryString() {
        throw new UnsupportedOperationException();
    }

}

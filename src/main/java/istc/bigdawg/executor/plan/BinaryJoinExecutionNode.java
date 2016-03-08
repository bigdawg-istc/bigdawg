package istc.bigdawg.executor.plan;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;

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
    public static class JoinOperand {
        public final ConnectionInfo engine;
        public final String table;
        public final String attribute;

        /**
         * Query to be executed assuming the other table is in a _RIGHTPARTIAL or _LEFTPARTIAL, and the result goes into a _RIGHTRESULT or _LEFTRESULT
         */
        private final String localJoinQuery;

        public JoinOperand(ConnectionInfo engine, String table, String attribute, String localJoinQuery) {
            this.engine = engine;
            this.table = table;
            this.attribute = attribute;
            this.localJoinQuery = localJoinQuery;
        }

        public String getQueryString() {
            return localJoinQuery;
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

        public String toString(){
            return "QUERY:" + this.getQueryString() + "TABLE:" + this.table + "ATTRIBUTE:" + this.attribute + "ENGINE:(" + ConnectionInfoParser.connectionInfoToString(this.engine) + ")";
        }

        public static JoinOperand stringTo(String rep){
            Pattern queryPat = Pattern.compile("(?<=QUERY:)(?s).*(?=TABLE:)");
            Pattern tablePat = Pattern.compile("(?<=TABLE:)(?s).*(?=ATTRIBUTE:)");
            Pattern attributePat = Pattern.compile("(?<=ATTRIBUTE:)(?s).*(?=ENGINE:)");
            Pattern enginePat = Pattern.compile("(?<=ENGINE:\\()[^\\)]*(?=\\))");

            String query = null;
            String table = null;
            String attribute = null;
            ConnectionInfo engine = null;

            Matcher m = queryPat.matcher(rep);
            if (m.find()) {
                query = m.group();
            }

            m = tablePat.matcher(rep);
            if (m.find()) {
                table = m.group();
            }

            m = attributePat.matcher(rep);
            if (m.find()) {
                attribute = m.group();
            }

            m = enginePat.matcher(rep);
            if (m.find()) {
                engine = ConnectionInfoParser.stringToConnectionInfo(m.group());
            }

            return new JoinOperand(engine, table, attribute, query);
        }
    }

    final JoinOperand left;
    final JoinOperand right;
    final Optional<JoinAlgorithms> hint;
    final ConnectionInfo destinationEngine;
    final String destinationTable;
    final String broadcastQuery;
    final String comparator;

    public BinaryJoinExecutionNode(String broadcastQuery, ConnectionInfo destinationEngine, String destinationTable, JoinOperand left, JoinOperand right, String comparator, Optional<JoinAlgorithms> hint) {
        this.broadcastQuery = broadcastQuery;
        this.destinationEngine = destinationEngine;
        this.destinationTable = destinationTable;
        this.left = left;
        this.right = right;
        this.hint = hint;
        this.comparator = comparator;
    }

    public BinaryJoinExecutionNode(String broadcastQuery, ConnectionInfo destinationEngine, String destinationTable, JoinOperand left, JoinOperand right, String comparator) {
        this(broadcastQuery, destinationEngine, destinationTable, left, right, comparator, Optional.empty());
    }

    public BinaryJoinExecutionNode(String broadcastQuery, ConnectionInfo destinationEngine, String destinationTable, JoinOperand left, JoinOperand right, String comparator, JoinAlgorithms hint) {
        this(broadcastQuery, destinationEngine, destinationTable, left, right, comparator, Optional.of(hint));
    }

    public String getShuffleUnionString(String leftResults, String rightResults) {
        return "SELECT * INTO " + this.destinationTable + " FROM " + leftResults + " UNION ALL SELECT * FROM " + rightResults + ";";
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

    public boolean isEquiJoin() {
        return this.comparator.equals("=");
    }

    public String toString(){
        StringBuilder currentRep = new StringBuilder();
        currentRep.append("(");
        currentRep.append(String.format("TABLE:%s", this.destinationTable));
        currentRep.append(String.format("QUERY:%s", this.broadcastQuery));
        currentRep.append(String.format("COMPARATOR:%s", this.comparator));
        currentRep.append(String.format("CONNECTION:(%s)", ConnectionInfoParser.connectionInfoToString(this.destinationEngine)));
        currentRep.append(String.format("LEFT:(%s)", this.left.toString()));
        currentRep.append(String.format("RIGHT:(%s)", this.right.toString()));
        currentRep.append(String.format("NODETYPE:%s", this.getClass().getName()));

        currentRep.append("HINT:");
        if (this.hint.isPresent()){
            currentRep.append(this.hint.get());
        }
        currentRep.append(")");
        return currentRep.toString();
    }

    public static BinaryJoinExecutionNode stringTo(String rep){
        Pattern tablePat = Pattern.compile("(?<=TABLE:)(?s).*(?=QUERY:)");
        Pattern queryPat = Pattern.compile("(?<=QUERY:)(?s).*(?=COMPARATOR:)");
        Pattern compPat = Pattern.compile("(?<=COMPARATOR:)(?s).*(?=CONNECTION:)");
        Pattern connPat = Pattern.compile("(?<=CONNECTION:\\()[^\\)]*(?=\\))");
        Pattern leftPat = Pattern.compile("(?<=LEFT:\\().*(?=\\)RIGHT:)");
        Pattern rightPat = Pattern.compile("(?<=RIGHT:\\().*(?=\\)NODETYPE:)");
        Pattern hintPat = Pattern.compile("(?<=HINT:)(?s)[^\\)]*");

        String table = null;
        String query = null;
        String comp = null;
        ConnectionInfo conn = null;
        JoinOperand left = null;
        JoinOperand right = null;
        JoinAlgorithms hint = null;

        Matcher m = tablePat.matcher(rep);
        if (m.find()) {
            table = m.group();
        }

        m = queryPat.matcher(rep);
        if (m.find()) {
            query = m.group();
        }

        m = compPat.matcher(rep);
        if (m.find()) {
            comp = m.group();
        }

        m = connPat.matcher(rep);
        if (m.find()) {
            conn = ConnectionInfoParser.stringToConnectionInfo(m.group());
        }

        m = leftPat.matcher(rep);
        if (m.find()) {
            left = JoinOperand.stringTo(m.group());
        }

        m = rightPat.matcher(rep);
        if (m.find()) {
            right = JoinOperand.stringTo(m.group());
        }

        m = hintPat.matcher(rep);
        if (m.find()) {
            String temp = m.group();
            if (temp.length() > 0){
                hint = JoinAlgorithms.valueOf(temp);
            }
        }

        if (hint != null){
            return new BinaryJoinExecutionNode(query, conn, table, left, right, comp, hint);
        }
        return new BinaryJoinExecutionNode(query, conn, table, left, right, comp);
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

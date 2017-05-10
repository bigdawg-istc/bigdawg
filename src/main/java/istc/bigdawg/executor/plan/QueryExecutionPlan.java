package istc.bigdawg.executor.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jgrapht.Graphs;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.islands.IslandAndCastResolver;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;

/**
 * Represents the steps needed to compute a query as a directed, acyclic graph
 * of ExecutionNodes and their dependencies.
 *
 * @author ankushg
 */
public class QueryExecutionPlan extends DirectedAcyclicGraph<ExecutionNode, DefaultEdge>
        implements Iterable<ExecutionNode> {

    private static final long serialVersionUID = 7704709501946249185L;
    private static int maxSerial = 0;
    private int serial;
    private String terminalTableName;
    private ExecutionNode terminalNode;
    
    private final Scope island;

    public QueryExecutionPlan(Scope island) {
        super(DefaultEdge.class);
        this.island = island;
        maxSerial++;
        serial = maxSerial;
        // TODO add any variables needed from Planner
    }

    public Scope getIsland() {
        return this.island;
    }
    
    public int getSerialNumber() {
    	return serial;
    }
    
    public String getSerializedName() {
    	return "BIGDAWGQEPTERMINALNODE_"+serial;
    }
    
    public void setTerminalTableName(String terminalTableName) {
    	this.terminalTableName = terminalTableName;
    }
    
    public String getTerminalTableName() {
    	return terminalTableName;
    }
    
    public void setTerminalTableNode(ExecutionNode terminalTable) {
    	this.terminalNode = terminalTable;
    }
    
    public ExecutionNode getTerminalTableNode() {
    	return this.terminalNode;
    }

    /**
     * @param node
     *            the ExecutionNode to get the dependencies of
     * @return a collection of the nodes that are the <b>immediate</b>
     *         dependencies of the specified node
     */
    public Collection<ExecutionNode> getDependencies(ExecutionNode node) {
        return Graphs.predecessorListOf(this, node);
    }

    /**
     * @param node
     *            the ExecutionNode to get the dependents of
     * @return a collection of the nodes that are the <b>immediate</b>
     *         dependents of the specified node
     */
    public Collection<ExecutionNode> getDependents(ExecutionNode node) {
        return Graphs.successorListOf(this, node);
    }

    /**
     * Adds the given node if not already present
     *
     * @param node
     *            ExecutionNode to be added to the execution plan
     * @return true if this plan did not already contain the specified node.
     */
    public boolean addNode(ExecutionNode node) {
        return this.addVertex(node);
    }

    /**
     * Adds all provided nodes if any is not already present, and marks the
     * dependencies as given. Does not remove any previous dependencies.
     *
     * @param node
     *            the ExecutionNode for which we are inserting dependencies
     * @param dependencies
     *            a list of ExecutionNodes that node depends on
     * @throws DirectedAcyclicGraph.CycleFoundException
     *             if any dependency would induce a cycle
     */
    public void addDependencies(ExecutionNode node, Collection<ExecutionNode> dependencies)
            throws DirectedAcyclicGraph.CycleFoundException {
        this.addNode(node);
        for (ExecutionNode dep : dependencies) {
            // adds the dependency iff not already present
            this.addNode(dep);

            // adds an edge from the dependency to the node
            this.addDagEdge(dep, node);
        }
    }

    /**
     * Produces a String representation of a QueryExecutionPlan
     * @param qep a QueryExecutionPlan
     * @return the String representation
     */
    public static String qepToString(QueryExecutionPlan qep) {
        Iterator<ExecutionNode> nodeIterator = qep.iterator();
        StringBuilder result = new StringBuilder();
        result.append(String.format("ISLAND:%s", qep.getIsland()));
        // Converts each node into a String representation of itself
        result.append("NODES:(");
        List<String> nodes = new ArrayList<>();
        while (nodeIterator.hasNext()) {
            ExecutionNode currentNode = nodeIterator.next();
            String currentNodeRep = ExecutionNodeFactory.executionNodeToString(currentNode);
            nodes.add(currentNodeRep);
        }
        Collections.sort(nodes);
        Map<String, Integer> order = new HashMap<>();
        int i = 0;
        for (String currentNodeRep: nodes) {
            order.put(currentNodeRep, i);
            i++;
            result.append(currentNodeRep);
        }
        result.append(")");

        // Stores the edges as tuples of the form (from,to) indexed by order added to the result
        result.append("EDGES:(");
        nodeIterator = qep.iterator();
        while (nodeIterator.hasNext()) {
            ExecutionNode currentNode = nodeIterator.next();
            int from = order.get(ExecutionNodeFactory.executionNodeToString(currentNode));
            Collection<ExecutionNode> dependents = qep.getDependents(currentNode);
            if (dependents != null) {
                for (ExecutionNode dependent: dependents) {
                    int to = order.get(ExecutionNodeFactory.executionNodeToString(dependent));
                    result.append(String.format("(%d,%d)", from, to));
                }
            }
        }
        
        result.append(")");

        result.append(String.format("SERIAL:%d", qep.getSerialNumber()));
        String termTable = "";
        if (qep.getTerminalTableName() != null){
            termTable = qep.getTerminalTableName();
        }
        result.append(String.format("TERMTABLE:%s", termTable));
        String termNode = "";
        if (qep.getTerminalTableNode() != null) {
            termNode = String.valueOf(order.get(ExecutionNodeFactory.executionNodeToString(qep.getTerminalTableNode())));
        }
        result.append(String.format("TERMNODE:%s", termNode));
        return result.toString();
    }

    /**
     * Produces an QueryExecutionPlan from the output of qepToString
     * @param representation an output of qepToString
     * @return the QueryExecutionPlan
     * @throws org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException 
     */
    public static QueryExecutionPlan stringToQEP(String representation) throws org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException {
        Pattern islandPattern = Pattern.compile("(?<=ISLAND:)(?s).*(?=NODES:)");
        Pattern nodesPattern = Pattern.compile("(?<=NODES:\\()(?s).*(?=\\)EDGES:)");
        Pattern edgesPattern = Pattern.compile("(?<=EDGES:\\()(?s).*(?=\\))");

        Pattern nodePattern = Pattern.compile("(\\(.*?ENGINE:\\(.*?\\).*?\\))");
        Pattern edgePattern = Pattern.compile("([0-9]+),([0-9]+)");

        Pattern serialPattern = Pattern.compile("(?<=SERIAL:)([0-9]+)");
        Pattern terminalTableNamePattern = Pattern.compile("(?<=TERMTABLE:)((?s).*)(?=TERMNODE:)");
        Pattern terminalNodePattern = Pattern.compile("(?<=TERMNODE:)([0-9]+)");

        String island = "";
        Matcher m = islandPattern.matcher(representation);
        if (m.find()) {
            island = m.group();
        }

        String nodes = "";
        m = nodesPattern.matcher(representation);
        if (m.find()) {
            nodes = m.group();
        }

        String edges = "";
        m = edgesPattern.matcher(representation);
        if (m.find()) {
            edges = m.group();
        }

        QueryExecutionPlan qep = new QueryExecutionPlan(Scope.valueOf(island));

        List<ExecutionNode> nodeList = new ArrayList<>();
        m = nodePattern.matcher(nodes);
        while (m.find()) {
            ExecutionNode currentNode = ExecutionNodeFactory.stringToExecutionNode(m.group());
            qep.addVertex(currentNode);
            nodeList.add(currentNode);
        }


        try {
            m = edgePattern.matcher(edges);
            while (m.find()) {
                int from = Integer.parseInt(m.group(1));
                int to = Integer.parseInt(m.group(2));
                qep.addDagEdge(nodeList.get(from), nodeList.get(to));
            }
        } catch (CycleFoundException e) {
            // Misformatted String
            e.printStackTrace();
            return null;
        }

        m = serialPattern.matcher(representation);
        if (m.find()) {
            String serial = m.group();
            qep.serial = Integer.parseInt(serial);
        }

        m = terminalTableNamePattern.matcher(representation);
        if (m.find()) {
            String terminalTableName = m.group();
            if (terminalTableName.length() > 0) {
                qep.setTerminalTableName(terminalTableName);
            }
        }

        m = terminalNodePattern.matcher(representation);
        if (m.find()) {
            String terminalNode = m.group();
            if (terminalNode.length() > 0) {
                qep.setTerminalTableNode(nodeList.get(Integer.parseInt(terminalNode)));
            }
        }

        return qep;
    }
}

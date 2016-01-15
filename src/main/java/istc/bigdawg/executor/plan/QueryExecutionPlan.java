package istc.bigdawg.executor.plan;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import org.jgrapht.Graphs;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

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
    
    private final String island;

    public QueryExecutionPlan(String island) {
        super(DefaultEdge.class);
        this.island = island;
        maxSerial++;
        serial = maxSerial;
        // TODO add any variables needed from Planner
    }

    public String getIsland() {
        return this.island;
    }
    
    public int getSerialNumber() {
    	return serial;
    }
    
    public String getSerializedName() {
    	return "QEPTERMINALNODE_"+serial;
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
    public void addDependencies(ExecutionNode node, List<ExecutionNode> dependencies)
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
     * Produces a String representation of an ExecutionNode
     * @param node The ExecutionNode we want to make into a String
     * @return The representation
     */
    public static String executionNodeToString(ExecutionNode node) {
        StringBuilder currentRep = new StringBuilder();
        currentRep.append("(");

        Optional<String> queryString = node.getQueryString();
        if (queryString.isPresent()) {
            currentRep.append("QUERY:");
            currentRep.append(queryString.get());
        }

        Optional<String> tableName = node.getTableName();
        if (tableName.isPresent()) {
            currentRep.append("TABLE:");
            currentRep.append(tableName.get());
        }

        currentRep.append("ENGINE:(");
        ConnectionInfo engine = node.getEngine();
        currentRep.append(String.format("CONNECTIONTYPE:%s", engine.getClass().getName()));
        currentRep.append(String.format("HOST:%s", engine.getHost()));
        currentRep.append(String.format("HANDLER:%s", engine.getHandler()));
        currentRep.append(String.format("PASSWORD:%s", engine.getPassword()));
        currentRep.append(String.format("PORT:%s", engine.getPort()));
        currentRep.append(String.format("USER:%s", engine.getUser()));
        currentRep.append(")");

        currentRep.append(String.format("NODETYPE:%s", node.getClass().getName()));
        currentRep.append(")");
        return currentRep.toString();
    }

    /**
     * Produces an ExecutionNode from the output of executionNodeToString
     * @param representation an output of executionNodeToString
     * @return the ExecutionNode
     */
    public static ExecutionNode stringToExecutionNode(String representation) {
        Pattern queryTable = Pattern.compile("(?<=QUERY:)(?s).*(?=TABLE:)");
        Pattern queryEngine = Pattern.compile("(?<=QUERY:)(?s).*(?=ENGINE:)");
        Pattern table = Pattern.compile("(?<=TABLE:)(?s).*(?=ENGINE:)");
        Pattern engine = Pattern.compile("(?<=ENGINE:\\()[^\\)]*(?=\\))");
        Pattern connectionType = Pattern.compile("(?<=CONNECTIONTYPE:)(?s).*(?=HOST:)");
        Pattern host = Pattern.compile("(?<=HOST:)(?s).*(?=HANDLER:)");
        Pattern handler = Pattern.compile("(?<=HANDLER:)(?s).*(?=PASSWORD:)");
        Pattern password = Pattern.compile("(?<=PASSWORD:)(?s).*(?=PORT:)");
        Pattern port = Pattern.compile("(?<=PORT:)(?s).*(?=USER:)");
        Pattern user = Pattern.compile("(?<=USER:)(?s).*");
        Pattern nodeType = Pattern.compile("(?<=NODETYPE:)[^\\)]*(?=\\))");

        // Extract the query
        Optional<String> query = Optional.empty();
        if (representation.contains("QUERY:")) {
            if (representation.contains("TABLE:")) {
                Matcher m = queryTable.matcher(representation);
                if (m.find()) {
                    query = Optional.of(m.group());
                }
            } else {
                Matcher m = queryEngine.matcher(representation);
                if (m.find()) {
                    query = Optional.of(m.group());
                }
            }
        }

        // Extract the tableName
        Optional<String> tableName = Optional.empty();
        if (representation.contains("TABLE:")){
            Matcher m = table.matcher(representation);
            if (m.find()) {
                tableName = Optional.of(m.group());
            }
        }

        // Extract the ConnectionInfo
        Matcher m = engine.matcher(representation);
        String engineInfo = "";
        if (m.find()) {
            engineInfo = m.group();
        }

        String engineConnectionType = "";
        m = connectionType.matcher(engineInfo);
        if (m.find()) {
            engineConnectionType = m.group();
        }
        String engineHost = "";
        m = host.matcher(engineInfo);
        if (m.find()) {
            engineHost = m.group();
        }
        String engineHandler = "";
        m = handler.matcher(engineInfo);
        if (m.find()) {
            engineHandler = m.group();
        }
        String enginePassword = "";
        m = password.matcher(engineInfo);
        if (m.find()) {
            enginePassword = m.group();
        }
        String enginePort = "";
        m = port.matcher(engineInfo);
        if (m.find()) {
            enginePort = m.group();
        }
        String engineUser = "";
        m = user.matcher(engineInfo);
        if (m.find()) {
            engineUser = m.group();
        }

        ConnectionInfo connectionInfo = null;
        // TODO implement for other ConnectionInfo classes
        if (engineConnectionType.contains("PostgreSQLConnectionInfo")){
            connectionInfo = new PostgreSQLConnectionInfo(engineHost, enginePort, engineHandler, engineUser, enginePassword);
        }

        // Get the type of ExecutionNode
        String nodeClass = "LocalQueryExecutionNode";
        m = nodeType.matcher(representation);
        if (m.find()) {
            nodeClass = m.group();
        }

        ExecutionNode result = null;
        if (nodeClass.contains("LocalQueryExecutionNode")) {
            result = new LocalQueryExecutionNode(query.get(), connectionInfo, tableName.get());
        } else if (nodeClass.contains("TableExecutionNode")) {
            result = new TableExecutionNode(connectionInfo, tableName.get());
        } else if (nodeClass.contains("BinaryJoinExecutionNode")) {
            result = new BinaryJoinExecutionNode();
        }
        return result;
    }

    /**
     * Produces a String representation of a QueryExecutionPlan
     * @param qep a QueryExecutionPlan
     * @return the String representation
     */
    public static String qepToString(QueryExecutionPlan qep) {
        Iterator<ExecutionNode> nodeIterator = qep.iterator();
        StringBuilder result = new StringBuilder();
//        System.out.println("Check point 1, qepToString");
        result.append(String.format("ISLAND:%s", qep.getIsland()));
//        System.out.println("Check point 2, qepToString");
        // Converts each node into a String representation of itself
        result.append("NODES:(");
        List<String> nodes = new ArrayList<>();
        while (nodeIterator.hasNext()) {
            ExecutionNode currentNode = nodeIterator.next();
            String currentNodeRep = executionNodeToString(currentNode);
            nodes.add(currentNodeRep);
        }
        Collections.sort(nodes);
//        System.out.println("Check point 3, qepToString");
        Map<String, Integer> order = new HashMap<>();
        int i = 0;
        for (String currentNodeRep: nodes) {
            order.put(currentNodeRep, i);
            i++;
            result.append(currentNodeRep);
        }
        result.append(")");
//        System.out.println("Check point 4, qepToString");
        // Stores the edges as tuples of the form (from,to) indexed by order added to the result
        
        
        result.append("EDGES:(");
        
        
        
        if (false) {
//        if (!qep.edgeSet().isEmpty()) {
	        nodeIterator = qep.iterator();
	        System.out.println("Check point 4.5, qepToString");
	        while (nodeIterator.hasNext()) {
	            ExecutionNode currentNode = nodeIterator.next();
	            System.out.printf("\ncurrentNode: %s\n", order.containsKey(currentNode));
	            System.out.printf("order: %s\n\n", order.containsKey(currentNode));
	            int from = order.get(executionNodeToString(currentNode));
	            Collection<ExecutionNode> dependents = qep.getDependents(currentNode);
	            if (dependents != null) {
	                for (ExecutionNode dependent: dependents) {
	                    int to = order.get(executionNodeToString(dependent));
	                    result.append(String.format("(%d,%d)", from, to));
	                }
	            }
	        }
        }
        
        
        
        
        
        result.append(")");
        
//        System.out.println("Check point 5, qepToString");
        result.append(String.format("SERIAL:%d", qep.getSerialNumber()));
        result.append(String.format("TERMTABLE:%s", qep.getTerminalTableName()));
        result.append(String.format("TERMNODE:%d", order.get(executionNodeToString(qep.getTerminalTableNode()))));
//        System.out.println("Check point 6, qepToString");
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

        QueryExecutionPlan qep = new QueryExecutionPlan(island);

        List<ExecutionNode> nodeList = new ArrayList<>();
        m = nodePattern.matcher(nodes);
        while (m.find()) {
            ExecutionNode currentNode = stringToExecutionNode(m.group());
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
            qep.setTerminalTableNode(nodeList.get(Integer.parseInt(terminalNode)));
        }

        return qep;
    }
}

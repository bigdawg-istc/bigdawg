package teddy.bigdawg.executor.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    private String queryId = "";
    private String island = "";

    public QueryExecutionPlan() {
        super(DefaultEdge.class);
        // TODO add any variables needed from Planner
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

    public void setQueryId(String queryId){
        this.queryId = queryId;
    }

    public String getQueryId(){
        return this.queryId;
    }

    public void setIsland(String island) {
        this.island = island;
    }

    public String getIsland() {
        return this.island;
    }
}

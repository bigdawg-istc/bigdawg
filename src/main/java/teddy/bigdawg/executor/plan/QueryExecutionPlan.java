package teddy.bigdawg.executor.plan;

import java.util.Iterator;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

/**
 * Represents the steps needed to compute a query as a directed, acyclic graph
 * of ExecutionNodes and their dependencies.
 * 
 * @author ankushg
 */
public class QueryExecutionPlan {
    private DirectedAcyclicGraph<ExecutionNode, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);

    public QueryExecutionPlan() {
        // TODO: add any instance variables needed to be passed from Planner to
        // Executor here (rather than passing as method parameters)
    }

    /**
     * Adds the given node if not already present
     * 
     * @param node
     *            ExecutionNode to be added to the execution plan
     * @return true if this plan did not already contain the specified node.
     */
    public boolean addNode(ExecutionNode node) {
        return graph.addVertex(node);
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
    public void addDependencies(ExecutionNode node, ExecutionNode... dependencies)
            throws DirectedAcyclicGraph.CycleFoundException {
        this.addNode(node);
        for (ExecutionNode dep : dependencies) {
            // adds the dependency iff not already present
            this.addNode(dep);

            // adds an edge from the dependency to the node
            graph.addDagEdge(dep, node);
        }
    }

    /**
     * iterator will traverse the nodes in topological order, meaning that if
     * node a depends on node b, then node a is guaranteed to come before node b
     * in the iteration order
     * 
     * @return an iterator that will traverse the graph in topological order
     */
    public Iterator<ExecutionNode> iterator() {
        return graph.iterator();
    }
}

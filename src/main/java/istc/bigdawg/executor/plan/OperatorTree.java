package istc.bigdawg.executor.plan;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.Set;

class OperatorTree extends DirectedAcyclicGraph<ExecutionNode, DefaultEdge> {
    public Set<ExecutionNode> entryPoints;
    public ExecutionNode exitPoint;

    public OperatorTree() {
        super(DefaultEdge.class);
    }
}
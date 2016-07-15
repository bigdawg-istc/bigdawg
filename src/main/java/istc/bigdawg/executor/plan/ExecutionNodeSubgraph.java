package istc.bigdawg.executor.plan;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

class ExecutionNodeSubgraph extends DirectedAcyclicGraph<ExecutionNode, DefaultEdge> {

	private static final long serialVersionUID = -8472224818432901514L;
	//    public Set<ExecutionNode> entryPoints;
    public ExecutionNode exitPoint;

    public ExecutionNodeSubgraph() {
        super(DefaultEdge.class);
    }
}
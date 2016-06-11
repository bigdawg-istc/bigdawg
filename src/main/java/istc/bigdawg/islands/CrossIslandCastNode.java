package istc.bigdawg.islands;

import java.util.List;

import istc.bigdawg.utils.IslandsAndCast.Scope;

public class CrossIslandCastNode extends CrossIslandPlanNode {
	
	protected Scope destinationScope;
	
	public CrossIslandCastNode(Scope sourceScope, Scope destinationScope, String schemaCreationQuery, String name) {
		super(sourceScope, schemaCreationQuery, name);
		this.destinationScope = destinationScope;
	}

	public Scope getDestinationScope() {
		return destinationScope;
	}

	public void setDestinationScope(Scope destinationScope) {
		this.destinationScope = destinationScope;
	}
	
	
	public CrossIslandPlanNode getSourceVertex(CrossIslandQueryPlan ciqp) throws Exception {
		List<CrossIslandPlanNode> source = getSourceOrTarget(ciqp, true);
		if (source.isEmpty()) return null;
		else return source.get(0);
	}
	
}

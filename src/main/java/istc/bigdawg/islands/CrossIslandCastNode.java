package istc.bigdawg.islands;

import java.util.List;

import istc.bigdawg.islands.IslandsAndCast.Scope;

public class CrossIslandCastNode extends CrossIslandPlanNode {
	
	protected Scope destinationScope;
	
	public CrossIslandCastNode(Scope sourceScope, Scope destinationScope, String schemaFilling, String name) throws Exception {
		super(sourceScope, schemaFilling, name);
		this.destinationScope = destinationScope;
		setQueryString(IslandsAndCast.getCreationQuery(destinationScope, name, schemaFilling));
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

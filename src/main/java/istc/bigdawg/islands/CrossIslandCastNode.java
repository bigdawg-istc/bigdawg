package istc.bigdawg.islands;

import java.util.List;

import istc.bigdawg.utils.IslandsAndCast.Scope;

public class CrossIslandCastNode extends CrossIslandPlanNode {
	
	protected Scope destinationScope;
	
	public CrossIslandCastNode(Scope sourceScope, Scope destinationScope, String schemaCreationQuery, String name) throws Exception {
		super(sourceScope, schemaCreationQuery, name);
		this.destinationScope = destinationScope;
		if (destinationScope.equals(Scope.ARRAY)) setQueryString(String.format("CREATE ARRAY %s %s", name, schemaCreationQuery));
		else if (destinationScope.equals(Scope.RELATIONAL)) setQueryString(String.format("CREATE TABLE %s %s", name, schemaCreationQuery));
		else throw new Exception ("Unsupported destination island in cast creation: "+destinationScope.name());
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

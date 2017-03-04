package istc.bigdawg.islands;

import java.util.List;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;

public class CrossIslandCast extends CrossIslandQueryNode {
	
	protected static int maxSerial = 0;
	protected int serial; 
	protected Scope destinationScope;
	
	public CrossIslandCast(Scope sourceScope, Scope destinationScope, String schemaFilling, String name) throws IslandException {
		super(sourceScope, schemaFilling, name);
		maxSerial++;
		serial = maxSerial;
		this.destinationScope = destinationScope;
		setQueryString(IslandAndCastResolver.getIsland(destinationScope)
				.getCreateStatementForTransitionTable(name, schemaFilling));
	}

	public Scope getDestinationScope() {
		return destinationScope;
	}

	public void setDestinationScope(Scope destinationScope) {
		this.destinationScope = destinationScope;
	}
	
	
	public CrossIslandQueryNode getSourceVertex(CrossIslandQueryPlan ciqp) throws Exception {
		List<CrossIslandQueryNode> source = getSourceOrTarget(ciqp, true);
		if (source.isEmpty()) return null;
		else return source.get(0);
	}
	
	@Override
	public String toString() {
		return String.format("(CICN_%s %s to %s)", serial, sourceScope.name(), destinationScope.name());
	}
}

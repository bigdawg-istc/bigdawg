package istc.bigdawg.islands;

import istc.bigdawg.islands.IslandAndCastResolver.Scope;

public class CrossIslandNonOperatorNode extends CrossIslandQueryNode {
	
	public CrossIslandNonOperatorNode (Scope sourceScope, String islandQuery, String name) {
		super(sourceScope, islandQuery, name);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(DummyNode ").append(this.sourceScope.name());
		sb.append(')');
		return sb.toString();
	}
}

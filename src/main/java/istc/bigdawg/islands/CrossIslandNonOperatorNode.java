package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.islands.IslandsAndCast.Scope;

public class CrossIslandNonOperatorNode extends CrossIslandPlanNode {
	
	private List<Object> additionalAttributes = new ArrayList<>();

	public CrossIslandNonOperatorNode (Scope sourceScope, String islandQuery, String name, List<Object> additionalAttributes) {
		super(sourceScope, islandQuery, name);
		this.additionalAttributes.addAll(additionalAttributes);
	}

	public List<Object> getAdditionalAttributes() {
		return additionalAttributes;
	}

	public void setAdditionalAttributes(List<Object> additionalAttributes) {
		this.additionalAttributes = additionalAttributes;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(DummyNode ").append(this.sourceScope.name());
		sb.append(' ').append(additionalAttributes); 
		sb.append(')');
		return sb.toString();
	}
}

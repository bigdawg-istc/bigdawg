package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.islands.IslandsAndCast.Scope;

public class CrossIslandUnionNode extends CrossIslandPlanNode {

	protected static int maxSerial = 0;
	protected int serial; 
	protected List<CrossIslandPlanNode> inputNodes;
	
	public CrossIslandUnionNode(Scope sourceScope, String name, List<CrossIslandPlanNode> inputNodes) {
		super(sourceScope, null, name);
		maxSerial++;
		serial = maxSerial;
		this.inputNodes = new ArrayList<>(inputNodes);
	}

	public List<CrossIslandPlanNode> getInputNodes() {
		return inputNodes;
	}

	public void setInputNodes(List<CrossIslandPlanNode> inputNodes) {
		this.inputNodes = inputNodes;
	}
	
}

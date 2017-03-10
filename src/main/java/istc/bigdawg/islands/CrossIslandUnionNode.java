package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.islands.IslandAndCastResolver.Scope;

public class CrossIslandUnionNode extends CrossIslandQueryNode {

	protected static int maxSerial = 0;
	protected int serial; 
	protected List<CrossIslandQueryNode> inputNodes;
	
	public CrossIslandUnionNode(Scope sourceScope, String name, List<CrossIslandQueryNode> inputNodes) {
		super(sourceScope, null, name);
		maxSerial++;
		serial = maxSerial;
		this.inputNodes = new ArrayList<>(inputNodes);
	}

	public List<CrossIslandQueryNode> getInputNodes() {
		return inputNodes;
	}

	public void setInputNodes(List<CrossIslandQueryNode> inputNodes) {
		this.inputNodes = inputNodes;
	}
	
}

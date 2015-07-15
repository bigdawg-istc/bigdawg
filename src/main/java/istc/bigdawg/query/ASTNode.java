package istc.bigdawg.query;

import istc.bigdawg.BDConstants.Island;
import istc.bigdawg.BDConstants.Operator;
import istc.bigdawg.BDConstants.Shim;

import java.util.ArrayList;
import java.util.List;

public class ASTNode {
	
	private List<ASTNode> children;
	private ASTNode parent;
	private String target; //object or query
	private Island island;
	private Shim shim;
	private Operator op;
	
	
	public ASTNode(String target, Island island, Shim shim, Operator op) {
		super();
		this.target = target;
		this.island = island;
		this.shim = shim;
		this.op = op;
		this.parent = null;
		this.children = null;;
	}
	public ASTNode(ASTNode parent, String target, Island island, Shim shim,
			Operator op) {
		super();
		this.parent = parent;
		this.children = null;
		this.target = target;
		this.island = island;
		this.shim = shim;
		this.op = op;
	}
	
	
	
	public List<ASTNode> getChildren() {
		return children;
	}
	public void addChild(ASTNode child) {
		if (this.children == null){
			this.children = new ArrayList<>();
		}
		this.children.add(child);
	}
	public ASTNode getParent() {
		return parent;
	}
	public void setParent(ASTNode parent) {
		this.parent = parent;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public Island getIsland() {
		return island;
	}
	public void setIsland(Island island) {
		this.island = island;
	}
	public Shim getShim() {
		return shim;
	}
	public void setShim(Shim shim) {
		this.shim = shim;
	}
	public Operator getOp() {
		return op;
	}
	public void setOp(Operator op) {
		this.op = op;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((children == null) ? 0 : children.hashCode());
		result = prime * result + ((island == null) ? 0 : island.hashCode());
		result = prime * result + ((op == null) ? 0 : op.hashCode());
		result = prime * result + ((parent == null) ? 0 : parent.hashCode());
		result = prime * result + ((shim == null) ? 0 : shim.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ASTNode other = (ASTNode) obj;
		if (children == null) {
			if (other.children != null)
				return false;
		} else if (!children.equals(other.children))
			return false;
		if (island != other.island)
			return false;
		if (op != other.op)
			return false;
		if (parent == null) {
			if (other.parent != null)
				return false;
		} else if (!parent.equals(other.parent))
			return false;
		if (shim != other.shim)
			return false;
		if (target == null) {
			if (other.target != null)
				return false;
		} else if (!target.equalsIgnoreCase(other.target))
			return false;
		return true;
	}
	
	

}

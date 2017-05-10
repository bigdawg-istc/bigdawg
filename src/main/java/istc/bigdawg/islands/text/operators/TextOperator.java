package istc.bigdawg.islands.text.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.exceptions.ShimException;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.shims.OperatorQueryGenerator;

public class TextOperator implements Operator {

	static final String subtreeToken = "BIGDAWGTEXTSUBTREE_";
	static final String pruneToken = "BIGDAWGTEXTPRUNED_";
	static Integer nextOperatorID = 0;
	Integer operatorID;
	
	boolean isPruned;
	boolean isSubTree;
	boolean isBlocking;
	boolean isQueryRoot;
	boolean isCopy;
	boolean isCTERoot;

	List<Operator> children;
	Operator parent; 
	
//	Map<String, DataObjectAttribute> outSchema;
	
	public TextOperator () {
		this.children = new ArrayList<>();
//		this.outSchema = new HashMap<>(); 
		this.operatorID = nextOperatorID++;
	}
	
	@Override
	public boolean isSubTree() {
		return isSubTree;
	}

	@Override
	public boolean isPruned() {
		return isPruned;
	}

	@Override
	public boolean isCTERoot() {
		return isCTERoot;
	}

	@Override
	public boolean isBlocking() {
		return isBlocking;
	}

	@Override
	public boolean isQueryRoot() {
		return isQueryRoot;
	}

	@Override
	public boolean isCopy() {
		return isCopy;
	}

	@Override
	public void setSubTree(boolean t) {
		isSubTree = t;
	}

	@Override
	public void prune(boolean p) {
		isPruned = p;
	}

	@Override
	public void setCTERoot(boolean b) {
		isCTERoot = b;
	}

	@Override
	public void setQueryRoot(boolean isQueryRoot) {
		this.isQueryRoot = isQueryRoot;
	}

	@Override
	public String getSubTreeToken() throws IslandException {
		return subtreeToken + this.operatorID;
	}

	@Override
	public String getPruneToken() throws IslandException {
		return pruneToken + this.operatorID;
	}

	@Override
	public Integer getPruneID() {
		return operatorID;
	}

	@Override
	public Integer getBlockerID() throws IslandException {
		return operatorID;
	}

	@Override
	public Operator getParent() {
		return parent;
	}

	@Override
	public List<Operator> getChildren() {
		return children;
	}

	@Override
	public void setParent(Operator p) {
		this.parent = p;
	}

	@Override
	public void addChild(Operator aChild) {
		children.add(aChild);
	}

	@Override
	public void addChilds(Collection<Operator> childs) {
		this.children.addAll(childs);
	}

//	public Map<String, DataObjectAttribute> getOutSchema() {
//		return outSchema;
//	}

	@Override
	public List<Operator> getAllBlockers() {
		List<Operator> subBlockers = new ArrayList<>();
		for (Operator o : children) {
			subBlockers.addAll(o.getAllBlockers());
		}
		if (this.isBlocking) {
			subBlockers.add(this);
		}
		return subBlockers;
	}

	@Override
	public Operator duplicate(boolean addChild) throws IslandException {
		throw new IslandException("TextOperator: duplicate not implemented; class: "+this.getClass());
	}

	@Override
	public Map<String, String> getDataObjectAliasesOrNames() throws IslandException {
		throw new IslandException("TextOperator: getDataObjectAliasesOrNames not implemented; class: "+this.getClass());
	}

	@Override
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws IslandException {
		throw new IslandException("TextOperator: removeCTEEntriesFromObjectToExpressionMapping not implemented; class: "+this.getClass());
	}

	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException {
		throw new IslandException("TextOperator: getTreeRepresentation not implemented; class: "+this.getClass());
	}

	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException {
		throw new IslandException("TextOperator: getDataObjectAliasesOrNames not implemented; class: "+this.getClass());
	}

	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		throw new ShimException("TextOperator: accept not implemented; class: "+this.getClass());
	}
	
}

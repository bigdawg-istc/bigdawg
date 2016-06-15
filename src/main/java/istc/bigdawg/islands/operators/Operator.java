package istc.bigdawg.islands.operators;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.OperatorVisitor;

public interface Operator {

	static final String BigDAWGPruneToken = "BIGDAWGPRUNED_"; 
	static final String BigDAWGSubtreeToken = "BIGDAWGSUBTREE_";
	
	public abstract Operator duplicate(boolean addChild) throws Exception;
	public boolean isCopy();

	public String getTreeRepresentation(boolean isRoot) throws Exception;
	
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception;
	
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws Exception;
	
	public boolean isCTERoot();
	public void setCTERoot(boolean b);
	
	public void setParent(Operator p);
	public Operator getParent();

	public List<Operator> getChildren();
	public void addChild(Operator aChild);
	public void addChilds(Collection<Operator> childs);
		
	public boolean isBlocking();
	public Integer getBlockerID() throws Exception;
	public List<Operator> getAllBlockers();
	
	public boolean isPruned();
	public void prune(boolean p);
	public String getPruneToken() throws Exception;
	
	public boolean isSubTree();
	public void setSubTree(boolean t);
	public String getSubTreeToken() throws Exception;

	public void setQueryRoot(boolean isQueryRoot);
	public boolean isQueryRoot();
	
	public Map<String, String> getDataObjectAliasesOrNames() throws Exception;
	
	public void accept(OperatorVisitor operatorVisitor) throws Exception;
	
}

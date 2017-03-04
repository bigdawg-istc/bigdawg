package istc.bigdawg.islands.operators;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.shims.OperatorQueryGenerator;

public interface Operator {
	
	/**
	 * This function indicate for the Executor whether this operator is the root of a sub-query that needs to be materialized
	 * @return
	 */
	public boolean isSubTree();
	public boolean isPruned();
	public boolean isCTERoot();
	public boolean isBlocking();
	public boolean isQueryRoot();
	public boolean isCopy();
	
	public void setSubTree(boolean t);
	public void prune(boolean p);
	public void setCTERoot(boolean b);
	public void setQueryRoot(boolean isQueryRoot);
	
	public String getSubTreeToken() throws IslandException;
	public String getPruneToken() throws IslandException;
	public Integer getPruneID();
	public Integer getBlockerID() throws IslandException;
	
	
	public Operator getParent();
	public List<Operator> getChildren();
	
	public void setParent(Operator p);
	public void addChild(Operator aChild);
	public void addChilds(Collection<Operator> childs);
	
	// FIXME consider this
//	public Map<String, SQLAttribute>  getOutSchema();
	
	/**
	 * get a list of all blocking operators among the offsprings of this operator  
	 * @return
	 */
	public List<Operator> getAllBlockers();
	
	/**
	 * Create a duplicate of this operator 
	 * @param addChild - add all child and progeny operators if true, none otherwise
	 * @return a duplicate of this operator
	 * @throws Exception
	 */
	public abstract Operator duplicate(boolean addChild) throws IslandException;
	
	/**
	 * The entries returned by this function should be a map between an object's alias and its original name
	 * @return
	 * @throws IslandException 
	 */
	public Map<String, String> getDataObjectAliasesOrNames() throws IslandException;
	
	/**
	 * This function is used for Signature construction. 
	 * Implementing it is necessary only when Common Table Expression (WITH) is supported.  
	 * @param entry
	 * @throws Exception
	 */
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws IslandException ;
	
	/**
	 * This function delivers parenthesize and simplified representation of the operator tree. 
	 * @param isRoot
	 * @return
	 * @throws Exception
	 */
	public String getTreeRepresentation(boolean isRoot) throws IslandException ;
	
	/**
	 * The entries should be aliases or original names of an object map to a set of all join predicates that contain references to the object  
	 * @return
	 * @throws Exception
	 */
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException ;
	
	/**
	 * Part of the visitor pattern that allows the generators to translate the operator tree into executable queries
	 * @param OperatorQueryGenerator
	 * @throws Exception
	 */
	public void accept(OperatorQueryGenerator generator) throws Exception;
	
}

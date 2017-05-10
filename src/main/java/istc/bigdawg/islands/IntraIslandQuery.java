package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.signature.Signature;

public abstract class IntraIslandQuery extends CrossIslandQueryNode {
	
	protected Signature signature;
	
	protected Map<String, QueryContainerForCommonDatabase> queryContainer;
	protected List<Operator> remainderPermutations;
	
	protected List<String> remainderLoc; // if null then we need to look into the container
	protected Map<String, List<String>> originalMap;
	protected Set<String> children;
	
	protected Operator initialRoot;
	
	public IntraIslandQuery (Scope scope, String islandQuery, String name, Map<String, String> transitionSchemas) throws Exception {
		super(scope, islandQuery, name);
		
		queryContainer = new HashMap<>();
		remainderPermutations = new ArrayList<>();
		remainderLoc = new ArrayList<>();
		
		// collect the cross island children
		children = getCrossIslandChildrenReferences(transitionSchemas);
	}
	
	/**
	 * This is only used for optimization.
	 * Evoked elsewhere and it will return null
	 * @return
	 */
	public Operator getInitialRoot() {
		return initialRoot;
	}
	
	public Set<String> getCrossIslandChildrenReferences(Map<String, String> transitionSchemas) {
		Set<String> offsprings = new HashSet<>(transitionSchemas.keySet());
		return offsprings;
	}
	
	public QueryExecutionPlan getQEP(int perm, boolean isSelect) throws Exception {
		
		// use perm to pick a specific permutation
		if (perm >= remainderPermutations.size()) throw new Exception ("Permutation reference index out of bound");
		
		QueryExecutionPlan qep = new QueryExecutionPlan(sourceScope); 
		ExecutionNodeFactory.addNodesAndEdgesNew(qep, remainderPermutations.get(perm), remainderLoc, queryContainer, isSelect, name);
		
		return qep;
	}
	
	public abstract List<QueryExecutionPlan> getAllQEPs(boolean isSelect) throws Exception;
	
	/**
	 * This function recursively traverse the original Operator Tree and determines if all the data sets are co-located.
	 * If it notices that two sub-trees locate on different engines, it will prune the two sub-trees and package 
	 * each of them into a container.
	 * @param node
	 * @param transitionSchemas
	 * @return a list of locations (DBID in Catalog) where all data sets could be found, or null if at least two data sets reside on different engines 
	 * @throws Exception
	 */
	protected abstract List<String> traverse(Operator node, Map<String, String> transitionSchemas) throws Exception;
	
	protected abstract void pruneChild(Operator c, List<String> traverseResult) throws Exception;
	
	public Set<String> getChildren() {
		return this.children;
	};
	
	public Operator getRemainder(int index) {
		return remainderPermutations.get(index);
	}
	
	public List<Operator> getAllRemainders() {
		return remainderPermutations;
	}
	
	public void setRemainders(List<Operator> remainderPermutations) {
		this.remainderPermutations = remainderPermutations;
	}
	
	public List<String> getRemainderLoc() {
		return remainderLoc;
	}
	
	public Map<String, QueryContainerForCommonDatabase> getQueryContainer(){
		return queryContainer;
	}
	
	public void setQueryContainer(Map<String, QueryContainerForCommonDatabase> queryContainer) {
		this.queryContainer = queryContainer;
	}
	
	public String getCrossIslandQueryNodeName() {
		return this.name;
	}
	
	public Signature getSignature() {
		return signature;
	}
	
	public void printSignature() {
		int i = 1;
		System.out.println("Signature Items: ");
		System.out.println("- "+i+++". remainder: "+signature.getSig1());
		System.out.println("- "+i+++". objects: "+signature.getSig2());
		System.out.println("- "+i+++". literals: "+signature.getSig3());
		for (String str : signature.getSig4k()) {
			System.out.println("- "+i+++". Container object: "+str);
		}
	}
	
	@Override
	public String toString() {
		return String.format("(CIQN %s (children %s))", name, children);
	}
	
}

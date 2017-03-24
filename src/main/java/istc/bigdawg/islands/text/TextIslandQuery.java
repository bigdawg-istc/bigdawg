package istc.bigdawg.islands.text;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.islands.Island;
import istc.bigdawg.islands.IslandAndCastResolver;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.islands.relational.RelationalIslandQuery;
import istc.bigdawg.signature.Signature;

public class TextIslandQuery extends IntraIslandQuery {

	public static final String tokenOfIndecision = "0";
	private static Logger logger = Logger.getLogger(RelationalIslandQuery.class);
	
	private Map<String, List<String>> originalMap;
	
	public TextIslandQuery(String islandQuery, String name, Map<String, String> transitionSchemas)
			throws Exception {
		super(Scope.TEXT, islandQuery, name, transitionSchemas);
		
		// collect the cross island children
		children = getCrossIslandChildrenReferences(transitionSchemas);
		
		logger.info(String.format("Island query: %s;", islandQuery));
		logger.info(String.format("--> CrossIsland children: %s;",children.toString()));
		logger.info(String.format("--> Transition schemas: %s;",transitionSchemas.toString()));
		
		Island thisIsland = IslandAndCastResolver.getIsland(Scope.TEXT);
		
		// create temporary tables that are used for as schemas
		thisIsland.setupForQueryPlanning(children, transitionSchemas);
		
		populateQueryContainer(thisIsland, transitionSchemas);
		
		// No need for optimization. the only thing we do now is scan, possibly with range
		List<Operator> theOnlyPermuation = new ArrayList<>();
		theOnlyPermuation.add(initialRoot);
		setRemainders(theOnlyPermuation);
		
		// create signature
		this.signature = new Signature(getQueryString(), getSourceScope(), getRemainder(0), getQueryContainer(), new HashSet<>());
		
		// deactivate initialRoot pointer
		initialRoot = null;
		
		// removing temporary schema plates
		thisIsland.teardownForQueryPlanning(children, transitionSchemas);
	}
	

	/** Setup. 
	 * 
	 * @throws Exception
	 */
	private void populateQueryContainer(Island thisIsland, Map<String, String> transitionSchemas) throws Exception {
		
		List<String> objs = new ArrayList<>();
		initialRoot = thisIsland.parseQueryAndExtractAllTableNames(queryString, objs);
		
		originalMap = CatalogViewer.getDBMappingByObj(objs, getSourceScope());
		
		// prune and populate containers
		remainderLoc = traverse(initialRoot, transitionSchemas); 
		logger.debug(String.format("Resulting remainder loc: %s", remainderLoc));
		
	}

	/**
	 * This function recursively traverse the original Operator Tree and determines if all the data sets are collocated.
	 * If it notices that two sub-trees locate on different engines, it will prune the two sub-trees and package 
	 * each of them into a container.
	 * @param node
	 * @param transitionSchemas
	 * @return a list of locations (DBID in Catalog) where all data sets could be found, or null if at least two data sets reside on different engines 
	 * @throws Exception
	 */
	protected List<String> traverse(Operator node, Map<String, String> transitionSchemas) throws Exception {

		List<String> ret = null;
		
		if (node instanceof SeqScan) {
			
			if ( transitionSchemas.containsKey(((SeqScan) node).getFullyQualifiedName())){
				
				logger.info(String.format("--> transitionSchema marked: %s\n", ((SeqScan) node).getFullyQualifiedName()));
				
				ret = new ArrayList<String>();
				ret.add(String.valueOf( IslandAndCastResolver.getIsland(sourceScope).getDefaultCastReceptionDBID()));
			} else {
				
				logger.info(String.format("--> printing qualified name: %s; originalMap: %s;", ((SeqScan) node).getFullyQualifiedName(), originalMap));
				
				if (originalMap.get(((SeqScan) node).getFullyQualifiedName()) != null)
					ret = new ArrayList<String>(originalMap.get(((SeqScan) node).getFullyQualifiedName()));
				else {
					logger.info(String.format("--> tokenOfIndecision evoked at SeqScan: %s\n", ((SeqScan) node).getFullyQualifiedName()));
					// in case it's not assigned
					ret = new ArrayList<> ();
					ret.add(tokenOfIndecision);
				}
				
			}
			
		} else {
			throw new Exception("Unsupported node: "+node.getClass().getName());
		}
		
		return ret;
	}
	
	protected void pruneChild(Operator c, List<String> traverseResult) throws Exception {
		// intentionally left blank
	}
	
	@Override
	public QueryExecutionPlan getQEP(int perm, boolean isSelect) throws Exception {
		
		// use perm to pick a specific permutation
		if (perm >= remainderPermutations.size()) throw new Exception ("Permutation reference index out of bound");
		
		QueryExecutionPlan qep = new QueryExecutionPlan(sourceScope); 
		ExecutionNodeFactory.addNodesAndEdgesNew(qep, remainderPermutations.get(perm), remainderLoc, queryContainer, isSelect, name);
		
		return qep;
	}
	
	@Override
	public List<QueryExecutionPlan> getAllQEPs(boolean isSelect) throws Exception {
		
		List<QueryExecutionPlan> qepl = new ArrayList<>();
		
		logger.info(String.format("RemainderPermuations, from getAllQEPs: %s\n", remainderPermutations));
		
		for (int i = 0; i < remainderPermutations.size(); i++ ){
			QueryExecutionPlan qep = new QueryExecutionPlan(getSourceScope()); 
			ExecutionNodeFactory.addNodesAndEdgesNew(qep, remainderPermutations.get(i), remainderLoc, queryContainer, isSelect, name);
			qepl.add(qep);
		}
		
		return qepl;
	}
	
	@Override
	protected Map<String, Set<Operator>> findIntersectionsSortByLargest(Map<Operator, Set<String>> traverseResults) {
		// intentionally left blank
		return null;
	}
		
}

package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Limit;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.islands.operators.Sort;
import istc.bigdawg.islands.operators.WindowAggregate;
import istc.bigdawg.islands.relational.operators.SQLIslandOperator;
import istc.bigdawg.islands.relational.operators.SQLIslandScan;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.signature.Signature;
import net.sf.jsqlparser.statement.select.Select;

public class CrossIslandQueryNode extends CrossIslandPlanNode {
	
	private static Logger logger = Logger.getLogger(CrossIslandQueryNode.class);
	public static final String tokenOfIndecision = "0";
	
	private Select select;
	private Signature signature;
	private DBHandler dbSchemaHandler = null;
	
	private Map<String, QueryContainerForCommonDatabase> queryContainer;

	private List<Operator> remainderPermutations;
	private List<String> remainderLoc; // if null then we need to look into the container
	
	private Map<String, List<String>> originalMap;
	private Set<String> children;
	
	private Set<String> originalJoinPredicates;
	private Set<String> joinPredicates;
	private Set<String> joinFilters;

	private Operator initialRoot;
	
	public CrossIslandQueryNode (IslandsAndCast.Scope scope, String islandQuery, String name, Map<String, String> transitionSchemas) throws Exception {
		super(scope, islandQuery, name);
		
		queryContainer = new HashMap<>();
		remainderPermutations = new ArrayList<>();
		remainderLoc = new ArrayList<>();
		joinPredicates = new HashSet<>();
		joinFilters = new HashSet<>();
		originalJoinPredicates = new HashSet<>();
		
		// collect the cross island children
		children = getCrossIslandChildrenReferences(transitionSchemas);
		
		logger.info(String.format("\n-> Island query: %s;\n", islandQuery));
		logger.info(String.format("---> CrossIslandChildren: %s;\n",children.toString()));
		logger.info(String.format("---> TransitionSchemas: %s;\n\n",transitionSchemas.toString()));
		
		// create temporary tables that are used for as schemas
		dbSchemaHandler = TheObjectThatResolvesAllDifferencesAmongTheIslands.createTableForPlanning(sourceScope, children, transitionSchemas);
		
		populateQueryContainer(scope, transitionSchemas);
		
		// OPTIMIZE
		CrossIslandQueryNodes.optimize(this);
		
		// create signature
		this.signature = new Signature(getQueryString(), getSourceScope(), getRemainder(0), getQueryContainer(), originalJoinPredicates);
		
		// deactivate initialRoot pointer
		initialRoot = null;
		
		// removing temporary schema plates
		TheObjectThatResolvesAllDifferencesAmongTheIslands.removeTemporaryTableCreatedForPlanning(sourceScope, dbSchemaHandler, children, transitionSchemas);
		
		
	}
	
	/** Setup. 
	 * 
	 * @throws Exception
	 */
	private void populateQueryContainer(Scope scope, Map<String, String> transitionSchemas) throws Exception {
		
		List<String> objs = new ArrayList<>();
		initialRoot = TheObjectThatResolvesAllDifferencesAmongTheIslands.generateOperatorTreesAndAddDataSetObjectsSignature(sourceScope, dbSchemaHandler, queryString, objs);
		
		originalJoinPredicates.addAll(getOriginalJoinPredicates(initialRoot));
		originalMap = CatalogViewer.getDBMappingByObj(objs, getSourceScope());
		
		// traverse add remainder
		if (initialRoot instanceof SQLIslandOperator) 
			logger.debug(String.format("Relational root schema before traverse: %s;", ((SQLIslandOperator)initialRoot).getOutSchema()));
		
		// prune and populate containers
		remainderLoc = traverse(initialRoot, transitionSchemas); 
		logger.debug(String.format("Resulting remainder loc: %s", remainderLoc));
		
	}
	
	/**
	 * This is only used for optimization.
	 * Evoked elsewhere and it will return null
	 * @return
	 */
	public Operator getInitialRoot() {
		return initialRoot;
	}
	
	public void setInitialRoot(Operator newRoot) {
		initialRoot = newRoot;
	}
	
	public Set<String> getCrossIslandChildrenReferences(Map<String, String> transitionSchemas) {

		// NEW METHOD
		Set<String> offsprings = new HashSet<>(transitionSchemas.keySet());
		return offsprings;
		// NEW METHOD END
		
	}
	
	public QueryExecutionPlan getQEP(int perm, boolean isSelect) throws Exception {
		
		// use perm to pick a specific permutation
		if (perm >= remainderPermutations.size()) throw new Exception ("Permutation reference index out of bound");
		
		QueryExecutionPlan qep = new QueryExecutionPlan(sourceScope); 
		ExecutionNodeFactory.addNodesAndEdgesNew(qep, remainderPermutations.get(perm), remainderLoc, queryContainer, isSelect, name);
		
		return qep;
	}
	
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

	private Set<String> getOriginalJoinPredicates(Operator root) throws Exception{
		
		Set<String> predicates = new HashSet<>();

		if (root == null) return predicates;

		String predicate = null;
		if (root instanceof Join){
			predicate = ((Join) root).generateJoinPredicate();
			if (predicate != null) predicates.addAll(TheObjectThatResolvesAllDifferencesAmongTheIslands.splitPredicates(sourceScope, predicate));
			predicate = ((Join) root).generateJoinFilter();
		} else if (root instanceof  Scan)
			predicate = ((Scan) root).generateRelevantJoinPredicate();
		
		if (predicate != null) predicates.addAll(TheObjectThatResolvesAllDifferencesAmongTheIslands.splitPredicates(sourceScope, predicate));

		for (Operator child: root.getChildren())
			predicates.addAll(getOriginalJoinPredicates(child));

		return predicates;
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
	private List<String> traverse(Operator node, Map<String, String> transitionSchemas) throws Exception {
		// now traverse nodes and group things together
		// So the remainders should be some things that does not contain individual nodes?
		// what about mimic2v26.d_patients join d?
		// what about. If d's associated queries are from a different database, then we give mimic2v26.d_patients a name,
		//   and store the skeleton that doesn't have any reference into the remainder?
		
		// seqscan: must be in the same server
		// join: if both from the same node then yes; otherwise part of the stem
		// sort: BLOCKING
		// aggregate: BLOCKING
		// xx distinct, aggregate, window aggregate: BLOCKING, if they are on the same node, then give as is; otherwise it's on the stem
		// xx With: check the parent node
		
		// for blocking: permutation must treat blocked branch as if it is pruned, but not actually prune it
		// 				 need to generate all possible permutations
		
		// for blocking: if child is on the same node, then check parent
		//                                             if on the same node, then all on the same node
		//											   otherwise, this operator on a node, parent is on the stem
		//       if child not on the same node, then all parent branches separated

		List<String> ret = null;
		
		if (node instanceof SeqScan) {
			
			if ( transitionSchemas.containsKey(((SeqScan) node).getFullyQualifiedName())){
				
				logger.info(String.format("--> transitionSchema marked: %s\n", ((SeqScan) node).getFullyQualifiedName()));
				
				ret = new ArrayList<String>();
				ret.add(String.valueOf(TheObjectThatResolvesAllDifferencesAmongTheIslands.getSchemaEngineDBID(sourceScope)));
			} else if (node.getChildren().size() > 0) {
				List<String> result = traverse(node.getChildren().get(0), transitionSchemas);
				if (result != null) ret = new ArrayList<String>(result); 
			} else {
				if (node instanceof SQLIslandScan && ((SQLIslandScan)node).getIndexCond() != null) {
					joinPredicates.add(((SQLIslandScan)node).getIndexCond().toString());
				}
				
				logger.info(String.format("--->> printing qualified name: %s; originalMap: %s; \n", ((SeqScan) node).getFullyQualifiedName(), originalMap));
				
				if (originalMap.get(((SeqScan) node).getFullyQualifiedName()) != null)
					ret = new ArrayList<String>(originalMap.get(((SeqScan) node).getFullyQualifiedName()));
				else {
					logger.info(String.format("--> tokenOfIndecision evoked at SeqScan: %s\n", ((SeqScan) node).getFullyQualifiedName()));
					// in case it's not assigned
					ret = new ArrayList<> ();
					ret.add(tokenOfIndecision);
				}
				
			}
			
		} else if (node instanceof Join) {
			
			
			Join joinNode = (Join) node;
			Operator child0 = joinNode.getChildren().get(0);
			Operator child1 = joinNode.getChildren().get(1);
			
			List <String> c0 = traverse(child0, transitionSchemas);
			List <String> c1 = traverse(child1, transitionSchemas);
			
			
			if (c0 != null && c1 != null) {
				
				Set <String> intersection = new HashSet<> (c0);
				intersection.retainAll(c1);
				
				if (intersection.isEmpty()) {
					
					if (c0.contains(tokenOfIndecision) ) {
						ret = new ArrayList<>(c1);
					} else if (c1.contains(tokenOfIndecision)) {
						ret = new ArrayList<>(c0);
					} else {
						pruneChild(child1, c1);
						pruneChild(child0, c0);
					}
					
				} else {
					ret = new ArrayList<>(intersection);
				}
			}
			
			
			if (c0 == null && c1 != null) {
				pruneChild(child1, c1);
			}
			
			if (c1 == null && c0 != null) {
				pruneChild(child0, c0);
			} 
			
			// do nothing if both are pruned before enter here, thus saving it for the remainder 
			
			if (joinNode.generateJoinPredicate() != null)
				joinPredicates.add(joinNode.generateJoinPredicate());//, child0, child1, new Table(), new Table(), true));
			if (joinNode.generateJoinFilter() != null)
				joinFilters.add(joinNode.generateJoinFilter());//, child0, child1, new Table(), new Table(), true));
			
		} else if (node instanceof Sort || node instanceof Aggregate || node instanceof Limit || node instanceof Distinct || node instanceof WindowAggregate) {
			
			// blocking come into effect
			List<String> result = traverse(node.getChildren().get(0), transitionSchemas);
			if (result != null) ret = new ArrayList<String>(result); 
		
		} else if (node instanceof Merge) {
			
			Merge mergeNode = (Merge) node;
			
			Map<Operator, Set<String>> traverseResults = new HashMap<>();
			List<Operator> nulled = new ArrayList<>();
			
			for (Operator o: mergeNode.getChildren()) {
				List <String> c = traverse(o, transitionSchemas);
				if (c == null) nulled.add(o);
				else traverseResults.put(o, new HashSet<>(c));
			}
			
			if (traverseResults.size() > 0) {
				// now the fancy largest sets problem...
				Map<String, Set<Operator>> intersections = findIntersectionsSortByLargest(traverseResults);
				
				// if there are more than one Entry, then break all of them into groups, make new Merges, prune
				if (intersections.size() == 1) {
					ret = new ArrayList<>((intersections.keySet()));
				} else {
					
					for (String s : intersections.keySet()) {
						List<String> ls = new ArrayList<>();
						ls.add(s);
						
						if (intersections.get(s).size() == 1) {
							pruneChild(intersections.get(s).iterator().next(), ls);
						} else {
							// for each group, make a new union; reset children and make parents 
							Set<Operator> so = intersections.get(s);
							Merge merge = (Merge) node.duplicate(false);
							for (Operator o : so) {
								node.getChildren().remove(o);
								o.setParent(merge);
							}
							merge.addChilds(so);
							node.addChild(merge);
							merge.setParent(node);
							
							pruneChild(merge, ls);
						}
					}
				}
				
			}
			
		} else {
			 throw new Exception("unsupported Operator in CrossIslandQueryNode");
		}
		
		return ret;
	}
	
	private void pruneChild(Operator c, List<String> traverseResult) throws Exception {
		// prune c
		c.prune(true);
		
		ConnectionInfo ci = null;
		String dbid = null;
		
//		if (traverseResult.size() > 1) {
//			throw new Exception("traverseResult size greater than 1: "+traverseResult);
//		}
		
		for (String s : traverseResult) {
			
			ci = CatalogViewer.getConnectionInfo(Integer.parseInt(s));
			if (ci == null) continue;
			dbid = s;
			if (ci != null) break;
			
		}
		
		queryContainer.put(c.getPruneToken(), new QueryContainerForCommonDatabase(ci, dbid, c, c.getPruneToken()));
	}
	
	private Map<String, Set<Operator>> findIntersectionsSortByLargest(Map<Operator, Set<String>> traverseResults) {
		
		Map<String, Set<Operator>> result = new HashMap<>();
		Map<String, Set<Operator>> dbids = new HashMap<>();
		
		for (Operator o : traverseResults.keySet()) {
			for (String s : traverseResults.get(o)) {
				if (dbids.containsKey(s)) {
					dbids.get(s).add(o);
				} else {
					Set<Operator> so = new HashSet<>();
					so.add(o);
					dbids.put(s, so);
				}
			}
		}
		
		// performance tip: allow multiple destination choices TODO 
		
		String maxString = null;
		Set<Operator> set = null;
		int maxCount = 0;
		
		while (!dbids.isEmpty()) {
			maxCount = 0;
			for (String s : dbids.keySet()) {
				int count = dbids.get(s).size();
				if (maxCount < count) {
					maxString = s;
					maxCount = count;
					set = new HashSet<>(dbids.get(s));
				}
			}
			result.put(maxString, new HashSet<>(set));
			
			// removal
			for (Operator o : set) {
				for (String s : traverseResults.get(o)) {
					dbids.get(s).remove(o);
					if (dbids.get(s).isEmpty()) dbids.remove(s);
				}
			}
		}
		
//		System.out.printf("----> findIntersectionsSortByLargest result: %s\n", result);
		
		return result;
	}
	
	public Set<String> getJoinPredicates(){
		return joinPredicates;
	}
	
	public Set<String> getJoinFilters() {
		return joinFilters;
	}
	
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
	
	public Select getOriginalSQLSelect() {
		return select;
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
	
	public List<CrossIslandPlanNode> getSourceVertices(CrossIslandQueryPlan ciqp) throws Exception {
		return getSourceOrTarget(ciqp, true);
	}
}

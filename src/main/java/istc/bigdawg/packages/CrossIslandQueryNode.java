package istc.bigdawg.packages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.plan.AFLQueryPlan;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.AFLPlanParser;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.operators.Aggregate;
import istc.bigdawg.plan.operators.Distinct;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Join.JoinType;
import istc.bigdawg.plan.operators.Limit;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.Scan;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.plan.operators.Sort;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.signature.builder.ArraySignatureBuilder;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import istc.bigdawg.utils.IslandsAndCast;
import istc.bigdawg.utils.IslandsAndCast.Scope;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Select;

public class CrossIslandQueryNode {
	
	private IslandsAndCast.Scope scope;
	private String query;
	private Select select;
	private String name;
	private Signature signature;
	
	private Map<String, QueryContainerForCommonDatabase> queryContainer;
	private List<Operator> remainderPermutations;
	private List<String> remainderLoc; // if null then we need to look into the container
	
	private Set<String> children;
	private Matcher tagMatcher;
	private DBHandler dbSchemaHandler = null;
	 
	private Map<String, List<String>> originalMap;

	private Set<String> originalJoinPredicates;

	private Set<String> joinPredicates;
	private Set<String> joinFilters;
	
	private static final int  psqlSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	private static final int  scidbSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getSciDBSchemaServerDBID();
	
	
	public CrossIslandQueryNode (IslandsAndCast.Scope scope, String islandQuery, String name, Map<String, Operator> rootsForSchemas) throws Exception {
		this.scope = scope;
		this.query = islandQuery;
		this.name  = name;
		
		// collect the cross island children
		children = getCrossIslandChildrenReferences();
		
//		System.out.println("CrossIslandChildren: "+children.toString());
//		System.out.println("RootsForSchemas: "+rootsForSchemas.toString());
		
		System.out.println("Island query: " + islandQuery);
		
		// create new tables or arrays for planning use
		if (scope.equals(Scope.RELATIONAL)) {
			this.select = (Select) CCJSqlParserUtil.parse(islandQuery);
			dbSchemaHandler = new PostgreSQLHandler(psqlSchemaHandlerDBID);
			for (String key : rootsForSchemas.keySet()) {
				if (children.contains(key)) {
					System.out.println("key: "+key+"; query: "+rootsForSchemas.get(key).generateSQLCreateTableStatementLocally(key)+"\n\n");
					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL(rootsForSchemas.get(key)
							.generateSQLCreateTableStatementLocally(key));
				}
			}
		} else if (scope.equals(Scope.ARRAY)) {
			dbSchemaHandler = new SciDBHandler(scidbSchemaHandlerDBID);
			for (String key : rootsForSchemas.keySet()) {
				if (children.contains(key)) {
					System.out.println("key: "+key+"; query: "+rootsForSchemas.get(key).generateSQLCreateTableStatementLocally(key)+"\n\n");
					((SciDBHandler)dbSchemaHandler).executeStatement(rootsForSchemas.get(key)
							.generateAFLCreateArrayStatementLocally(key));
				}
			}
		} else
			throw new Exception("Unsupported island code : "+scope.toString());
		
		queryContainer = new HashMap<>();
		remainderPermutations = new ArrayList<>();
		remainderLoc = new ArrayList<>();
		joinPredicates = new HashSet<>();
		joinFilters = new HashSet<>();
		originalJoinPredicates = new HashSet<>();
		populateQueryContainer();
		
		
		this.signature = new Signature(islandQuery, scope, getRemainder(0), getQueryContainer(), originalJoinPredicates);
		
		
		// removing temporary schema plates
		if (scope.equals(Scope.RELATIONAL)) {
			for (String key : rootsForSchemas.keySet()) 
				if (children.contains(key)) 
					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL("drop table "+key);
		} else if (scope.equals(Scope.ARRAY)) {
			for (String key : rootsForSchemas.keySet()) 
				if (children.contains(key)) 
					((SciDBHandler)dbSchemaHandler).executeStatement("remove("+key+")");
		} else
			throw new Exception("Unsupported island code : "+scope.toString());
		
		
	}

	public Set<String> getJoinPredicates(){
		return this.joinPredicates;
	}
	
	public Set<String> getCrossIslandChildrenReferences() {
		
		// aka find children
		
		tagMatcher = Pattern.compile("\\bBIGDAWGTAG_[0-9]+\\b").matcher(query);
		Set<String> offsprings = new HashSet<>();
		
		while (tagMatcher.find()) {
			offsprings.add(query.substring(tagMatcher.start(), tagMatcher.end()));
		}
		
		return offsprings;
	}
	
	public QueryExecutionPlan getQEP(int perm, boolean isSelect) throws Exception {
		
		
		
		// use perm to pick a specific permutation
		if (perm >= remainderPermutations.size()) throw new Exception ("Permutation reference index out of bound");
		
		QueryExecutionPlan qep = new QueryExecutionPlan(scope); 
		ExecutionNodeFactory.addNodesAndEdgesWithJoinHandling(qep, remainderPermutations.get(perm), remainderLoc, queryContainer, isSelect); 
//		ExecutionNodeFactory.addNodesAndEdgesNaive( qep, remainderPermutations.get(perm), remainderLoc, queryContainer);
		
		return qep;
	}
	
	public List<QueryExecutionPlan> getAllQEPs(boolean isSelect) throws Exception {
		
		List<QueryExecutionPlan> qepl = new ArrayList<>();
		
		for (int i = 0; i < remainderPermutations.size(); i++ ){
			QueryExecutionPlan qep = new QueryExecutionPlan(scope); 
			ExecutionNodeFactory.addNodesAndEdgesWithJoinHandling(qep, remainderPermutations.get(i), remainderLoc, queryContainer, isSelect); 
//			ExecutionNodeFactory.addNodesAndEdgesNaive( qep, remainderPermutations.get(i), remainderLoc, queryContainer);
			qepl.add(qep);
		}
		
		return qepl;
	}
	
	/** Setup. 
	 * 
	 * @throws Exception
	 */
	private void populateQueryContainer() throws Exception {
		
		// NOW WE ONLY SUPPORT RELATIONAL ISLAND
		// SUPPORT OTHER ISLANDS && ISLAND CHECK 
		
		Operator root = null;
		ArrayList<String> objs = null;
		
		System.out.println("Original query to be parsed: \n"+query);
		
		if (scope.equals(Scope.RELATIONAL)) {
			SQLQueryPlan queryPlan = SQLPlanParser.extractDirect((PostgreSQLHandler)dbSchemaHandler, query);
			root = queryPlan.getRootNode();
			objs = new ArrayList<>(RelationalSignatureBuilder.sig2(query));
		} else if (scope.equals(Scope.ARRAY)) {
			AFLQueryPlan queryPlan = AFLPlanParser.extractDirect((SciDBHandler)dbSchemaHandler, query);
			root = queryPlan.getRootNode();
			objs = new ArrayList<>(ArraySignatureBuilder.sig2(query));
		} else 
			throw new Exception("Unsupported island code: "+scope.toString());

		originalJoinPredicates.addAll(getOriginalJoinPredicates(root));

		originalMap = CatalogViewer.getDBMappingByObj(objs);
		
		// traverse add remainder
		Map<String, DataObjectAttribute> rootOutSchema = root.getOutSchema();
		remainderLoc = traverse(root); // this populated everything
		
		Map<String, Map<String, String>> jp = processJoinPredicates(joinPredicates);
		Map<String, Map<String, String>> jf = processJoinPredicates(joinFilters);
		
		
		if (remainderLoc == null && root.getDataObjectNames().size() > 1) {
			
			List<Operator> permResult = getPermutatedOperatorsWithBlock(root, jp, jf);
			
			// if root is join then the constructed out schema might get messed up
			for (Operator o : permResult) {
				if (o.getOutSchema().size() != rootOutSchema.size()) {
					o.updateOutSchema(rootOutSchema);
				}
			}
//			// debug
//			System.out.println("\n\n\nResult of Permutation: ");
//			int i = 1;
//			for (Operator o : permResult) {
//				if (scope.equals(Scope.RELATIONAL))
//					System.out.printf("%d. %s\n\n", i, o.generateSQLString(select));
//				else if (scope.equals(Scope.ARRAY))
//					System.out.printf("%d. %s\n\n", i, o.generateAFLString(0));
//				
////				System.out.printf("%d. %s\n\n", i, o.generateSQLString(select)); // duplicate command to test modification of underlying structure
//				i++;
//			}
			
			remainderPermutations.clear();
			remainderPermutations.addAll(permResult);
			
//			// debug
//			remainderPermutations.add(root);
			
		} // if remainderLoc is not null, then THERE IS NO NEED FOR PERMUTATIONS 
		
	}

	private Set<String> getOriginalJoinPredicates(Operator root){
		Set<String> predicates = new HashSet<>();

		if (root == null){
			return predicates;
		}

		if (root instanceof Join){
			String predicate = ((Join) root).getOriginalJoinPredicate();
			if (predicate != null){
				predicates.addAll(splitPredicates(predicate));
			}

			predicate = ((Join) root).getOriginalJoinFilter();
			if (predicate != null){
				predicates.addAll(splitPredicates(predicate));
			}

		} else if (root instanceof  Scan){
			String predicate = ((Scan) root).getJoinPredicate();
			if (predicate != null){
				predicates.addAll(splitPredicates(predicate));
			}
		}

		for (Operator child: root.getChildren()){
			predicates.addAll(getOriginalJoinPredicates(child));
		}

		return predicates;
	}

	private static Set<String> splitPredicates(String predicates){
		Set<String> results = new HashSet<>();
		Pattern predicatePattern = Pattern.compile("(?<=\\()([^\\(^\\)]+)(?=\\))");
		Matcher m = predicatePattern.matcher(predicates);
		while (m.find()){
			results.add(m.group().replace(" ", ""));
		}
		return results;
	}
	
	
	private List<Operator> getPermutatedOperatorsWithBlock(Operator root, Map<String, Map<String, String>> joinPredConnections,  Map<String, Map<String, String>> joinFilterConnections) throws Exception {
		
		/**
		 * This function dictates which part of the tree could be permuted.
		 * Note, only Join and Scan type of operators are NOT blocking. 
		 */
		
		List<Operator> extraction = new ArrayList<>();
		List<Operator> blockers   = new ArrayList<>();
		List<Operator> leaves 	  = new ArrayList<>();

		
		if (root.blockingStatus()) {
			
			// then it must have only one child, because join does not block
			// root spear-heads the rest of the subtree
			
			System.out.println("--> blocking root; class: "+root.getClass().getSimpleName()+"; ");
			System.out.println("--> tree rep: "+root.getTreeRepresentation(true)+"; ");
			System.out.println("--> SQL: "+root.generateSQLString(null)+"; \n");
			
			Operator next = root.getChildren().get(0);
			while (!(next instanceof Join)) next = next.getChildren().get(0);
			
			List<Operator> ninos = getPermutatedOperatorsWithBlock(next, joinPredConnections, joinFilterConnections);
			
			for (Operator o: ninos) {
				
				Operator t = root.duplicate(true); // TODO USED TO BE FALSE

				extraction.add(t);
				
				t.getChildren().get(0).setParent(t);
				while (!(t instanceof Join)) {
					t = t.getChildren().get(0);
					t.getChildren().get(0).setParent(t);
				}
				t = t.getParent();
				t.getChildren().clear();
				t.addChild(o);
				
			}
			
		} else {
			
			// add all leaves and blockers to the lists, 
			
			List<Operator> treeWalker = root.getChildren();
			while(treeWalker.size() > 0) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator c : treeWalker) {
					
					// prune and block will never collide
					if (c.isPruned()) {
						
						leaves.add(c);
						
					} else if (c.blockingStatus()) {
						
						Operator t = c.duplicate(true); // TODO USED TO BE FALSE
						leaves.add(t);
						blockers.add(t);
						
					} else {
						nextGeneration.addAll(c.getChildren());
					}
					
				}
				
				treeWalker = nextGeneration;
			}
			
			/**
			 *  now we have all the blockers and leaves
			 *  1. permute the leaves
			 *  2. run this function on all blockers to get a list of lists of blocker trees
			 *  3. locate the blockers in the permuted leaves and REPLACE them with the computed blocker trees
			 */
			
			// 1.
			List<Operator> permutationsOfLeaves = getPermutatedOperators(leaves, joinPredConnections, joinFilterConnections);
			Map<Integer, List<Operator>> blockerTrees = new HashMap<>();
			
			// 2.
			for (Operator b : blockers) {
				blockerTrees.put(b.getBlockerID(), getPermutatedOperatorsWithBlock(b, joinPredConnections, joinFilterConnections));
			}
			
			// 3.
			int repeats = 1;
			Map<Integer, Integer> startingPoints = new HashMap<>();
			for (Integer blkrID : blockerTrees.keySet()) {
				repeats *= blockerTrees.get(blkrID).size();
				startingPoints.put(blkrID, 0);
			}
			
			if (blockerTrees.size() > 0){
				for (Operator pl : permutationsOfLeaves) {
					
					
					for (int i = 0; i < repeats; ++i) {
						
						/** 
						 * find the corresponding blocker list,
						 * replace the children list with the found list
						 * repeat for all in the blkrs
						 * after done, add dupe to extraction
						 */
						
						Operator dupe = pl.duplicate(true);
						
						List<Operator> blkrs = dupe.getAllBlockers();
						
						boolean increment = false;
						boolean base = true;
						
						for (Operator op : blkrs) {
							
							Integer id = op.getBlockerID();
							
							int pos = startingPoints.get(id);
							
							op.getChildren().clear();
							op.getChildren().addAll(blockerTrees.get(id).get(pos).getChildren());
							
							// increment the startingPoints

							if (base) {
								increment = incrementStartingPoints(id, pos, blockerTrees, startingPoints);
								base = false;
							} else if (increment) {
								increment = incrementStartingPoints(id, pos, blockerTrees, startingPoints);
							}
						}
						
						extraction.add(dupe);
					}
				}
			} else {
				extraction.addAll(permutationsOfLeaves);
			}
		}
		return extraction;
	}
	
	private boolean incrementStartingPoints(Integer id, Integer pos, Map<Integer, List<Operator>> blockerTrees, Map<Integer, Integer> startingPoints) {
		if (pos + 1 == blockerTrees.get(id).size()) {
			startingPoints.replace(id, 0);
			return true;
		} else {
			startingPoints.replace(id, pos + 1);
			return false;
		}
	}
	
	
	private List<Operator> getPermutatedOperators(List<Operator> ops, Map<String, Map<String, String>> joinPredConnections, Map<String, Map<String, String>> joinFilterConnections) throws Exception {
		
		/**
		 * Eventually 'ops' should be able to contain blocking operators,
		 * and permuted blocking operators themselves should be able to go back
		 * through the nest
		 */
		
		
		List<Operator> extraction = new ArrayList<>();
		
		int len = ops.size();
		

		if (len == 1) {
			extraction.add(ops.get(0));
//			// debug
//			System.out.println("---------- case of one: "+ops.get(0).getOutSchema().toString());
			return extraction;
			
		} else if (len == 2) {
			// the case of two
			extraction.add(makeJoin(ops.get(0), ops.get(1), null, joinPredConnections, joinFilterConnections, new HashSet<>())); 
			return extraction;
		} 
		
		ArrayList<List<Operator>> permutations = new ArrayList<>();
		permutations.add(ops);
		
		ArrayList<Operator> newEntries;
		
		int j0;
		int j1;
		int k0;
		int k1;
		for (int i = 1; i < len ; i ++ ) {
			newEntries = new ArrayList<Operator>();

			for (j0 = i - 1; j0 >= 0 ; j0 --) {
				// j0 and j1 are the two to be joined
				j1 = i - j0 - 1; 
				
				if (j0 < j1) break; // we always want the larger, j0, in the front
				if (j0 == j1) {
					// iterate only when j0's sub position is smaller, and they contain distinct members
					
					List<Operator> j0list = permutations.get(j0);
					int j0listSize = j0list.size();
					
					for (k0 = 0; k0 < j0listSize; k0 ++ ) {
						Operator k0o = permutations.get(j0).get(k0);
						Operator k1o;
						for (k1 = k0 + 1; k1 < j0listSize; k1 ++) {
							k1o = permutations.get(j0).get(k1);
							
							if (isDisjoint(k0o, k1o)) { // disjoint is on the progeny is required
								
								
//								// debug
//								if (i == len-1) {
//									l ++;
//									System.out.printf("--->>>>>>> equal %d. ", l);
//								}
								
								addNewEntry(k0o, k1o, joinPredConnections, joinFilterConnections, newEntries);
								 
							}
							// else do nothing
						}
					}
				} else {
					// iterate through all
					List<Operator> j0list = permutations.get(j0);
					List<Operator> j1list = permutations.get(j1);
					int j0listSize = j0list.size();
					int j1listSize = j1list.size();
					
					for (k0 = 0; k0 < j0listSize; k0 ++ ) {
						Operator k0o = permutations.get(j0).get(k0);
						Operator k1o;
						for (k1 = 0; k1 < j1listSize; k1 ++) {

							k1o = permutations.get(j1).get(k1);
							
							if (isDisjoint(k0o, k1o)) {
								
//								// debug
//								if (i == len-1) {
//									l ++;
//									System.out.printf("--->>>>>>> not %d. ", l);
//								}
								addNewEntry(k0o, k1o, joinPredConnections, joinFilterConnections, newEntries);
							}
						}
					}
				}
			}
			permutations.add(newEntries);
		}
		
		extraction.addAll(permutations.get(permutations.size()-1));
		
		
		return extraction;
	}

	/**
	 * Add new entry for permutation
	 * Note: use children characteristics to avoid making unnecessary entries.
	 * @param k0o
	 * @param k1o
	 * @param joinPredConnections
	 * @param joinFilterConnections
	 * @param newEntry
	 * @throws Exception
	 */
	private void addNewEntry (Operator k0o, Operator k1o, Map<String, Map<String, String>> joinPredConnections, Map<String, Map<String, String>> joinFilterConnections, List<Operator> newEntry) throws Exception {
		
		if (k1o instanceof Join) {
			// all on-expression must precede cross-joins
			if (k0o instanceof Join && (((Join)k0o).getCurrentJoinPredicate() == null && ((Join)k1o).getCurrentJoinPredicate() != null)) {
				return;
			}
				
		
			if ( ((Join)k1o).getCurrentJoinPredicate() != null || ((Join)k1o).getCurrentJoinFilter() != null) {
				Set<String> objlist1 = new HashSet<>(k0o.getDataObjectAliasesOrNames().keySet());
				Set<String> objlist2 = new HashSet<>(objlist1);
				String jp = ((Join)k1o).getCurrentJoinPredicate();
				String jf = ((Join)k1o).getCurrentJoinFilter();
				
				boolean jpb = false;
				boolean jfb = false;
				
				if (jp != null) jpb = objlist1.removeAll( SQLExpressionUtils.getColumnTableNamesInAllForms(CCJSqlParserUtil.parseCondExpression(jp)) );
				if (jf != null) jfb = objlist2.removeAll( SQLExpressionUtils.getColumnTableNamesInAllForms(CCJSqlParserUtil.parseCondExpression(jf)) );
				
				if (jpb || jfb) { 
					newEntry.add(makeJoin(k0o, k1o, null, joinPredConnections, joinFilterConnections, k0o.getDataObjectAliasesOrNames().keySet()));
				}
			}
		} else {
			
			if ((k0o.isPruned() || k0o instanceof Scan) && (!k1o.isPruned() && !(k1o instanceof Scan))) {
				// pruned non-left-deep branch
				return;
			}
			
			Operator ret = makeJoin(k0o, k1o, null, joinPredConnections,  joinFilterConnections, k0o.getDataObjectAliasesOrNames().keySet());
			if (ret == null) return; // the final prune done
			newEntry.add(ret);
		}
	}
	
	private Operator makeJoin(Operator o1, Operator o2, JoinType jt, Map<String, Map<String, String>> joinPredConnection, Map<String, Map<String, String>> joinFilterConnection, Set<String> used) throws Exception {
		
		
		Map<String, Map<String, String>> jp = new HashMap<>();
		Map<String, Map<String, String>> jf = new HashMap<>();
		
		for (String k : joinPredConnection.keySet()) {
			jp.put(k, new HashMap<>());
			for (String kin : joinPredConnection.get(k).keySet()) {
				jp.get(k).put(kin, joinPredConnection.get(k).get(kin));
			}
		}
		
		for (String k : joinFilterConnection.keySet()) {
			jf.put(k, new HashMap<>());
			for (String kin : joinFilterConnection.get(k).keySet()) {
				jf.get(k).put(kin, joinFilterConnection.get(k).get(kin));
			}
		}
		
		Set<String> o1ns = new HashSet<>(o1.getDataObjectAliasesOrNames().keySet());
		Set<String> o2nsOriginal = new HashSet<>(o2.getDataObjectAliasesOrNames().keySet());
		Set<String> o2ns = new HashSet<>(o2nsOriginal);
		
		o1ns.retainAll(Sets.union(jp.keySet(), jf.keySet()));
		
		Operator o1Temp = o1;
		Operator o2Temp = o2;
		
		if (o1.isCopy()) o1Temp = new Join(o1, true);
		if (o2.isCopy()) o2Temp = new Join(o2, true);
		
		for (String s : o1ns) {
			
			if (jp.get(s) == null) continue; // because jc is modified along the way
			
			o2ns.retainAll(Sets.union(jp.keySet(), jf.keySet()));
			
			if (!o2ns.isEmpty()) {
				
				String key = o2ns.iterator().next();
				
				while (used.contains(key)) {
					key = o2ns.iterator().next();
				}
				
				boolean isFilter = false;
				String pred;
				if (jp.get(s) != null) {
					pred = jp.get(s).get(key);
					jp.get(s).remove(key);
					jp.get(key).remove(s);
				} else {
					pred = jf.get(s).get(key);
					jf.get(s).remove(key);
					jf.get(key).remove(s);
					isFilter = true;
				} 
				
				// this is the final checking of whether we need to prune it
				if (o1Temp instanceof Join && (!(o2Temp instanceof Join)) 
						&& (((Join) o1Temp).getCurrentJoinPredicate() == null) && (((Join) o1Temp).getCurrentJoinFilter() == null)) {
					
					return null;
				}
					
				return new Join(o1Temp, o2Temp, jt, pred, isFilter);
			}
			
			o2ns = new HashSet<>(o2nsOriginal);
		}
		return new Join(o1Temp, o2Temp, jt, null, false);
	}
	
	private boolean isDisjoint(Operator s1, Operator s2) throws Exception {
		Set<String> set1 = new HashSet<String>(s1.getDataObjectNames());
		Set<String> set2 = new HashSet<String>(s2.getDataObjectNames());
		return (!(set1.removeAll(set2)));
	}
	
	
	
	
	
	
	private Map<String, Map<String, String>> processJoinPredicates(Set<String> jp) throws Exception {
		
		Map<String, Map<String, String>> result = new HashMap<>();
		
		for (String s : jp ) {
			
			Expression e = CCJSqlParserUtil.parseCondExpression(s);
			List<Column> lc = SQLExpressionUtils.getAttributes(e);
			
			String temp0 = lc.get(0).getTable().getFullyQualifiedName(); // we don't look for other dots because everything is pruned
			String temp1 = lc.get(1).getTable().getFullyQualifiedName();
			
			if (!result.containsKey(temp0))
				result.put(temp0, new HashMap<>());
			if (!result.containsKey(temp1))
				result.put(temp1, new HashMap<>());
			
			result.get(temp0).put(temp1, s);
			result.get(temp1).put(temp0, s);
		}
		
		return result;
	}
	
	
	private List<String> traverse(Operator node) throws Exception, Exception {
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

		ArrayList<String> ret = null;
		
		if (node instanceof SeqScan) {
			
			if (((SeqScan) node).getTable().getFullyQualifiedName().toLowerCase().startsWith("bigdawgtag_")){
				ret = new ArrayList<String>();
				if (scope.equals(Scope.RELATIONAL))
					ret.add(String.valueOf(psqlSchemaHandlerDBID));						// TODO IMPORTANT. CHANGE ORIGINAL MAPPING TO INCLUDE THIS
				else if (scope.equals(Scope.ARRAY))
					ret.add(String.valueOf(scidbSchemaHandlerDBID));
				else 
					throw new Exception("Unsupported island: "+scope.name());
			}else 
				ret = new ArrayList<String>(originalMap.get(((SeqScan) node).getTable().getFullyQualifiedName()));
			
			
		} else if (node instanceof Join) {
			
			
			Join joinNode = (Join) node;
			Operator child0 = joinNode.getChildren().get(0);
			Operator child1 = joinNode.getChildren().get(1);
			
			List <String> c0 = traverse(child0);
			List <String> c1 = traverse(child1);
			
			
			if (c0 != null && c1 != null) {
				
				Set <String> intersection = new HashSet<> (c0);
				intersection.retainAll(c1);
				
				if (intersection.isEmpty()) {
					pruneChild(child1, c1);
					pruneChild(child0, c0);
					
				} else {
					ret = new ArrayList<String>(intersection);
				}
			}
			
			
			if (c0 == null && c1 != null) {
				pruneChild(child1, c1);
			}
			
			if (c1 == null && c0 != null) {
				pruneChild(child0, c0);
			} 
			
			// do nothing if both are pruned before enter here, thus saving it for the remainder 
			
			if (joinNode.getOriginalJoinPredicate() != null)
				joinPredicates.add(joinNode.updateOnExpression(joinNode.getOriginalJoinPredicate(), child0, child1, new Table(), new Table(), true));
			if (joinNode.getOriginalJoinFilter() != null)
				joinPredicates.add(joinNode.updateOnExpression(joinNode.getOriginalJoinFilter(), child0, child1, new Table(), new Table(), true));
			
		} else if (node instanceof Sort || node instanceof Aggregate || node instanceof Limit || node instanceof Distinct) {
			
			// blocking come into effect
			List<String> result = traverse(node.getChildren().get(0));
			if (result != null) ret = new ArrayList<String>(result); 
		
		} else {
			 throw new Exception("unsupported Operator in CrossIslandQueryNode");
		}
		
		
		if (node.isQueryRoot()) {
			remainderPermutations.add(node);
		}
		
		return ret;
	}
	
	public void pruneChild(Operator c, List<String> traverseResult) throws Exception {
		// prune c
		c.prune(true);
		
		ConnectionInfo ci = null;
		String dbid = null;
		
		if (traverseResult.size() > 1)
			throw new Exception("traverseResult size greater than 1");
		
		for (String s : traverseResult) {
			
			if (scope.equals(Scope.RELATIONAL)) {
				ci = CatalogViewer.getPSQLConnectionInfo(Integer.parseInt(s));
				dbid = s;
			} else if (scope.equals(Scope.ARRAY)) {
				ci = CatalogViewer.getSciDBConnectionInfo(Integer.parseInt(s));
				dbid = s;
			} else 
				throw new Exception("Unsupported island code: "+scope.toString());
		}
		
		queryContainer.put(c.getPruneToken(), new QueryContainerForCommonDatabase(ci, dbid, c, c.getPruneToken()));
	}
	
	public Set<String> getChildren() {
		return this.children;
	};
	
	/**
	 * Get scope of this Node. It could be an island token or a 'cast'
	 * @return
	 */
	public IslandsAndCast.Scope getScope() {
		return this.scope;
	}
	
	public String getQuery() {
		return this.query;
	}
	
	public Operator getRemainder(int index) {
		return remainderPermutations.get(index);
	}
	
	public List<Operator> getAllRemainders() {
		return remainderPermutations;
	}
	
	public List<String> getRemainderLoc() {
		return remainderLoc;
	}
	
	public Map<String, QueryContainerForCommonDatabase> getQueryContainer(){
		return queryContainer;
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
}

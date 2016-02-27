package istc.bigdawg.packages;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.plan.AFLQueryPlan;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.AFLPlanParser;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Join.JoinType;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.plan.operators.Sort;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.signature.builder.ArraySignatureBuilder;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import istc.bigdawg.utils.IslandsAndCast;
import istc.bigdawg.utils.IslandsAndCast.Scope;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Select;

public class CrossIslandQueryNode {
	
	private IslandsAndCast.Scope scope;
	private String query;
	private Select select;
	private String name;
	
	private Map<String, QueryContainerForCommonDatabase> queryContainer;
	private List<Operator> remainderPermutations;
	private List<String> remainderLoc; // if null then we need to look into the container
	
	private Set<String> children;
	private Matcher tagMatcher;
	private DBHandler dbSchemaHandler = null;
	 
	private Map<String, List<String>> originalMap;
	
	private Set<String> joinPredicates;
	
	
	public CrossIslandQueryNode (IslandsAndCast.Scope scope, String islandQuery, String name, Map<String, Operator> rootsForSchemas) throws Exception {
		this.scope = scope;
		this.query = islandQuery;
		this.name  = name;
		
		// collect the cross island children
		children = getCrossIslandChildrenReferences();
		
//		System.out.println("CrossIslandChildren: "+children.toString());
//		System.out.println("RootsForSchemas: "+rootsForSchemas.toString());
		
		
		
		// create new tables or arrays for planning use
		if (scope.equals(Scope.RELATIONAL)) {
			this.select = (Select) CCJSqlParserUtil.parse(islandQuery);
			dbSchemaHandler = new PostgreSQLHandler(3);
			for (String key : rootsForSchemas.keySet()) {
				if (children.contains(key)) {
					System.out.println("key: "+key+"; query: "+rootsForSchemas.get(key).generateSQLCreateTableStatementLocally(key)+"\n\n");
					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL(rootsForSchemas.get(key)
							.generateSQLCreateTableStatementLocally(key));
				}
			}
		} else if (scope.equals(Scope.ARRAY)) {
			dbSchemaHandler = new SciDBHandler(6);
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
		populateQueryContainer();
		
		
		
		
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
	
	public Set<String> getCrossIslandChildrenReferences() {
		
		// aka find children
		
		tagMatcher = Pattern.compile("\\bBIGDAWGTAG_[0-9]+\\b").matcher(query);
		Set<String> offsprings = new HashSet<>();
		
		while (tagMatcher.find()) {
			offsprings.add(query.substring(tagMatcher.start(), tagMatcher.end()));
		}
		
		return offsprings;
	}
	
	public QueryExecutionPlan getQEP(int perm) throws Exception {
		
		
		
		// use perm to pick a specific permutation
		if (perm >= remainderPermutations.size()) throw new Exception ("Permutation reference index out of bound");
		
		
//		Select srcStmt = (Select) CCJSqlParserUtil.parse(query);
		QueryExecutionPlan qep = new QueryExecutionPlan(scope); 
		ExecutionNodeFactory.addNodesAndEdgesNaive( qep, remainderPermutations.get(perm), remainderLoc, queryContainer);
		
		return qep;
	}
	
	public List<QueryExecutionPlan> getAllQEPs() throws Exception {
		
		List<QueryExecutionPlan> qepl = new ArrayList<>();
		
		// after permutation, the same SELECT this might not work
		// but we'll see
//		Select srcStmt = (Select) CCJSqlParserUtil.parse(query);
		
		for (int i = 0; i < remainderPermutations.size(); i++ ){
			QueryExecutionPlan qep = new QueryExecutionPlan(scope); 
			ExecutionNodeFactory.addNodesAndEdgesNaive( qep, remainderPermutations.get(i), remainderLoc, queryContainer);
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
			objs = new ArrayList<>(Arrays.asList(RelationalSignatureBuilder.sig2(query).split("\t")));
		} else if (scope.equals(Scope.ARRAY)) {
			AFLQueryPlan queryPlan = AFLPlanParser.extractDirect((SciDBHandler)dbSchemaHandler, query);
			root = queryPlan.getRootNode();
			objs = new ArrayList<>(Arrays.asList(ArraySignatureBuilder.sig2(query).split("\t")));
		} else 
			throw new Exception("Unsupported island code: "+scope.toString());
		
		
		originalMap = CatalogViewer.getDBMappingByObj(objs);
		
		
		
		// traverse add remainder
		remainderLoc = traverse(root); // this populated everything
		
		Map<String, Map<String, String>> jp = processJoinPredicates(joinPredicates);
		
		
		if (remainderLoc == null && root.getDataObjectNames().size() > 1) {

			
			// Permutation happens here! it should use remainderPermutaions.get(0) as a basis and generate all other possible ones.
			// 1. support WITH ; 
			// 2. CHANGE SORT RELATED CODE
			//
			// Permutations are done according to the structure of remainders.
			// if remainderLoc is not null, then there is only one permutation
			// otherwise, one should be able to specify an index to construct a QEP
			
			// get on expressions for every join;
			// figure out possible relative ordering -- which one must be presented first and which one later --- pruning?
			// :: joinPredicates
			// convert joinPredicates into a set of rules? TODO
			// convert into 
			
			
			List<Operator> permResult = getPermutatedOperatorsWithBlock(root, jp);
			
			System.out.println("\n\n\nResult of Permutation: ");
			int i = 1;
			for (Operator o : permResult) {
				if (scope.equals(Scope.RELATIONAL))
					System.out.printf("%d. %s\n\n", i, o.generateSQLString(select));
				else if (scope.equals(Scope.ARRAY))
					System.out.printf("%d. %s\n\n", i, o.generateAFLString(0));
				
//				System.out.printf("%d. %s\n\n", i, o.generateSQLString(select)); // duplicate command to test modification of underlying structure
				i++;
			}
			
			
			remainderPermutations.clear();
			remainderPermutations.addAll(permResult);
			
//			// debug
//			remainderPermutations.add(root);
			
		} // if remainderLoc is not null, then THERE IS NO NEED FOR PERMUTATIONS 
		
	}
	
	
	private List<Operator> getPermutatedOperatorsWithBlock(Operator root, Map<String, Map<String, String>> joinPredConnections) throws Exception {
		
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
			
			List<Operator> ninos = getPermutatedOperatorsWithBlock(root.getChildren().get(0), joinPredConnections);
			
			for (Operator o: ninos) {
				
				Operator t = root.duplicate(false);
				t.addChild(o);
				extraction.add(t);
				
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
						
						Operator t = c.duplicate(false);
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
			List<Operator> permutationsOfLeaves = getPermutatedOperators(leaves, joinPredConnections);
			Map<Integer, List<Operator>> blockerTrees = new HashMap<>();
			
			// 2.
			for (Operator b : blockers) {
				blockerTrees.put(b.getBlockerID(), getPermutatedOperatorsWithBlock(b, joinPredConnections));
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
				// it is as is
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
	
	
	private List<Operator> getPermutatedOperators(List<Operator> ops, Map<String, Map<String, String>> joinPredConnections) throws Exception {
		
		/**
		 * Eventually 'ops' should be able to contain blocking operators,
		 * and permuted blocking operators themselves should be able to go back
		 * through the nest
		 */
		
		
		List<Operator> extraction = new ArrayList<>();
		
		int len = ops.size();
		

		if (len == 1) {
			// the case of one
			extraction.add(ops.get(0));
			
			System.out.println("---------- case of one: "+ops.get(0).getOutSchema().toString());
			
			
			return extraction;
			
		} else if (len == 2) {
			// the case of two
			extraction.add(makeJoin(ops.get(0), ops.get(1), null, joinPredConnections, new HashSet<>())); 
			return extraction;
		} 
		
		ArrayList<List<Operator>> permutations = new ArrayList<>();
		permutations.add(ops);
		
		ArrayList<Operator> newEntry;
		
		int j0;
		int j1;
		int k0;
		int k1;
		for (int i = 1; i < len ; i ++ ) {
			// i+1, start at 2, is the permutation tuples we're working on
			newEntry = new ArrayList<Operator>();
			
			

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
							
							if (isDisj(k0o, k1o)) {
								
								
//								// debug
//								if (i == len-1) {
//									l ++;
//									System.out.printf("--->>>>>>> equal %d. ", l);
//								}
								
								// condition being, the right does not have entry, or that it does and contains some on the left
								// if the left is also a join, 
								
								addNewEntry(k0o, k1o, joinPredConnections, newEntry);
								 
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
							
							if (isDisj(k0o, k1o)) {
								
								// TODO this is where you check conditions
								// well, currently we do not prune search space
								
//								// debug
//								if (i == len-1) {
//									l ++;
//									System.out.printf("--->>>>>>> not %d. ", l);
//								}
								
								addNewEntry(k0o, k1o, joinPredConnections, newEntry);
								
							}
						}
					}
				}
			}
			
			permutations.add(newEntry);
			
		}
		
		extraction.addAll(permutations.get(permutations.size()-1));
		
		
		return extraction;
	}

	private void addNewEntry (Operator k0o, Operator k1o, Map<String, Map<String, String>> joinPredConnections, List<Operator> newEntry) throws Exception {
		
		if (k1o instanceof Join) {
			if (k0o instanceof Join && (((Join)k0o).getCurrentJoinPredicate() == null && ((Join)k1o).getCurrentJoinPredicate() != null)) {
				
				return;
			}
				
		
			if ( ((Join)k1o).getCurrentJoinPredicate() != null) {
				Set<String> objlist = new HashSet<>(k0o.getDataObjectNames());
				String jp = ((Join)k1o).getCurrentJoinPredicate();
				if (objlist.removeAll(new HashSet<>(Arrays.asList(jp.split("[. =<>]+"))))) { 
					newEntry.add(makeJoin(k0o, k1o, null, joinPredConnections, k0o.getDataObjectNames()));
				}
			}
		} else {
			
			Operator ret = makeJoin(k0o, k1o, null, joinPredConnections, k0o.getDataObjectNames());
			if (ret == null) return; // the final prune done
			newEntry.add(ret);
		}
	}
	
	private Operator makeJoin(Operator o1, Operator o2, JoinType jt, Map<String, Map<String, String>> joinPredConnection, Set<String> used) throws Exception {
		
		
		Map<String, Map<String, String>> jc = new HashMap<>();
		
		for (String k : joinPredConnection.keySet()) {
			jc.put(k, new HashMap<>());
			for (String kin : joinPredConnection.get(k).keySet()) {
				jc.get(k).put(kin, joinPredConnection.get(k).get(kin));
			}
		}
		
		Set<String> o1ns = new HashSet<>(o1.getDataObjectNames());
		Set<String> o2nsOriginal = new HashSet<>(o2.getDataObjectNames());
		Set<String> o2ns = new HashSet<>(o2nsOriginal);
		
		o1ns.retainAll(jc.keySet());
		
//		System.out.println("\n\n\nSTARTS HERE!!!\n");
//		
		
		Operator o1Temp = o1;
		Operator o2Temp = o2;
		
		if (o1.isCopy()) o1Temp = new Join(o1, true);
		if (o2.isCopy()) o2Temp = new Join(o2, true);
		
		
		for (String s : o1ns) {
			
			if (jc.get(s) == null) continue; // because jc is modified along the way
			
			Set<String> ks = jc.get(s).keySet();
			
//			System.out.printf("old o2ns: %s\n", o2ns);
			
			o2ns.retainAll(ks);
			
			
//			System.out.printf("s: %s;     keySet: %s;       \nget: %s;      o2ns: %s\n", s, ks, jc.get(s), o2ns);
			
			if (!o2ns.isEmpty()) {
				
				
				String key = o2ns.iterator().next();
				
//				System.out.println("key: "+key);
				
				
				while (used.contains(key)) {
					key = o2ns.iterator().next();
//					System.out.println("key: "+key);
				}
				
				String pred = jc.get(s).get(key);
				jc.get(s).remove(key);
				jc.get(key).remove(s);
				
//				System.out.println("\n\nENDS HERE, MATCH!!!");
//				System.out.printf("o1 data and o2 data: %s     %s \n\n\n", o1.getDataObjectNames(), o2.getDataObjectNames());
//				
				// this is the final checking of whether we need to prune it
				if (o1Temp instanceof Join && (!(o2Temp instanceof Join)) 
						&& (((Join) o1Temp).getCurrentJoinPredicate() == null)) {
					
					return null;
				}
					
				
				return new Join(o1Temp, o2Temp, jt, pred);
			}
			
			o2ns = new HashSet<>(o2nsOriginal);
		}
		
		
//		System.out.println();
//		System.out.println("\n\nENDS HERE, NO MATCH!!!");
//		System.out.printf("o1 data and o2 data: %s     %s \n\n\n", o1.getDataObjectNames(), o2.getDataObjectNames());
		return new Join(o1Temp, o2Temp, jt, null);
	}
	
	private boolean isDisj(Operator s1, Operator s2) throws Exception {
		Set<String> set1 = new HashSet<String>(s1.getDataObjectNames());
		Set<String> set2 = new HashSet<String>(s2.getDataObjectNames());
		return (!(set1.removeAll(set2)));
	}
	
	
	
	
	
	
	private Map<String, Map<String, String>> processJoinPredicates(Set<String> jp) {
		
		Map<String, Map<String, String>> result = new HashMap<>();
		
		for (String s : jp ) {
			
			List<String> spl = Arrays.asList(s.split(" *([<>!=]+) *"));
			
			String temp0 = Arrays.asList((spl.get(0)).split("\\.")).get(0); // we don't look for other dots because everything is pruned
			String temp1 = Arrays.asList((spl.get(1)).split("\\.")).get(0);
			
			if (!result.containsKey(temp0))
				result.put(temp0, new HashMap<>());
			if (!result.containsKey(temp1))
				result.put(temp1, new HashMap<>());
			
//			String connector = s.substring(spl.get(0).length(), s.length()-spl.get(1).length());
			
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
		// xx aggregate: BLOCKING
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
			
//			System.out.println(((SeqScan) node).getTable().getFullyQualifiedName());
//			System.out.println(originalMap);
			
			if (((SeqScan) node).getTable().getFullyQualifiedName().toLowerCase().startsWith("bigdawgtag_")){
				ret = new ArrayList<String>();
				ret.add("3");
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
			
			if (((Join)node).getJoinPredicateOriginal() != null && (!((Join)node).getJoinPredicateOriginal().isEmpty()))
				joinPredicates.add(((Join)node).updateOnExpression(((Join)node).getJoinPredicateOriginal(), child0, child1, new Table(), new Table(), true));
			
		} else if (node instanceof Sort) {
			
			// blocking come into effect
			List<String> result = traverse(node.getChildren().get(0));
			if (result != null) ret = new ArrayList<String>(result); 
		
		} else {
			 throw new Exception("unsupported Operator in CrossIslandQueryNode");
		}
		
		
		if (node.isQueryRoot()) {
			// this status is set at SQLQueryPlan
			remainderPermutations.add(node);
		}
		
		return ret;
	}
	
	public void pruneChild(Operator c, List<String> traverseResult) throws Exception {
		// prune c
		c.prune(true);
		
		Map<String, ConnectionInfo> cis = new HashMap<>();
		
//		System.out.println("traverse result: "+traverseResult);
		
		for (String s : traverseResult) {
			
//			System.out.println("dbid: "+s);
			
			if (scope.equals(Scope.RELATIONAL))
				cis.put(s, CatalogViewer.getPSQLConnectionInfo(Integer.parseInt(s)));
			else if (scope.equals(Scope.ARRAY))
				cis.put(s, CatalogViewer.getSciDBConnectionInfo(Integer.parseInt(s)));
			else 
				throw new Exception("Unsupported island code: "+scope.toString());
		}
		
		queryContainer.put(c.getPruneToken(), new QueryContainerForCommonDatabase(cis, c, c.getPruneToken()));
		// ^ container added prior to root traverse
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
	
	public Map<String, QueryContainerForCommonDatabase> getQueryContainer(){
		return queryContainer;
	}
}

package istc.bigdawg.islands.SciDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import com.google.common.collect.Sets;

import istc.bigdawg.islands.IslandAndCastResolver;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandOperator;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

public class ArrayIslandPermuter {
	
	public static void optimize(ArrayIslandQuery ciqn) throws Exception {
		permute(ciqn);
	}
	
	
	private static void permute(ArrayIslandQuery ciqn) throws Exception {
		
		List<Operator> remainderPermutations = new ArrayList<>();
		
		Map<Pair<String, String>, String> jp = processJoinPredicates(ciqn.getJoinPredicates());
		Map<Pair<String, String>, String> jf = processJoinPredicates(ciqn.getJoinFilters());
		
		List<Set<String>> predicateConnections = populatePredicateConnectionSets(jp, jf);
		assert(ciqn.getInitialRoot() instanceof SciDBIslandOperator);
		SciDBIslandOperator root = (SciDBIslandOperator)ciqn.getInitialRoot(); 
		
		if (ciqn.getRemainderLoc() == null && root.getDataObjectAliasesOrNames().size() > 1) {
			List<Operator> permResult = getPermutatedOperatorsWithBlock(ciqn.getSourceScope(), root, jp, jf, predicateConnections);
			
			
//			// for debugging
//			System.out.println("\n\n\nResult of Permutation: ");
//			int i = 1;
//			OperatorVisitor gen;
//			for (Operator o : permResult) {
//				gen = TheObjectThatResolvesAllDifferencesAmongTheIslands.getQueryGenerator(sourceScope);
//				gen.configure(true, false);
//				o.accept(gen);
//				System.out.printf("%d.  first formulation: %s\n", i, gen.generateStatementString());
//				System.out.printf("%d. second formulation: %s\n", i, gen.generateStatementString()); // duplicate command to test modification of underlying structure
//				System.out.printf("--> o schema after gens: %s;\n\n", ((SQLIslandOperator) o).getOutSchema());
//				i++;
//			}
//			System.out.println("\n");
//			// end of debug
			
			remainderPermutations.addAll(permResult);
			
		} // if remainderLoc is not null, then THERE IS NO NEED FOR PERMUTATIONS 
		else {
			remainderPermutations.add(root);
		}
		
		ciqn.setRemainders( remainderPermutations);
	}
	
	/**
	 * This function constructs intra-island Join permutations of the original remainder Operator tree. 
	 * It treats subtrees whose root is pruned or blocks (Aggregate, Merge, Sort, etc.) 
	 * as leaves and construct the permutations by iteratively connecting the leaves with Join Operators. 
	 * base on the layout of the Join Operator that connect them.
	 * @param scope
	 * @param root
	 * @param joinPredConnections
	 * @param joinFilterConnections
	 * @return A list of Operators that are each a root of a permuted remainder Operator tree.
	 * @throws Exception
	 */
	private static List<Operator> getPermutatedOperatorsWithBlock(Scope scope, Operator root, Map<Pair<String, String>, String> joinPredConnections,  Map<Pair<String, String>, String> joinFilterConnections, List<Set<String>> predicateConnections) throws Exception {
		
		List<Operator> extraction = new ArrayList<>();
		List<Operator> blockers   = new ArrayList<>();
		List<Operator> leaves 	  = new ArrayList<>();
		
		if (root.isBlocking() ) {
//			// DEBUG ONLY
//			OperatorVisitor gen = null;
//			if (getSourceScope().equals(Scope.RELATIONAL)) {
//				gen = new SQLQueryGenerator();
//				((SQLQueryGenerator)gen).setSrcStatement(select);
//			}
//			gen.configure(true, false);
//			root.accept(gen);
//			
//			System.out.println("--> blocking root; class: "+root.getClass().getSimpleName()+"; ");
//			System.out.println("--> tree rep: "+root.getTreeRepresentation(true)+"; ");
//			System.out.println("--> SQL: "+gen.generateStatementString()+"; \n");
//			// DEBUG OUTPUT END
			
			
			boolean hasPermutation = false;
			List<List<Operator>> combos = new ArrayList<>();	// holder of operators
			for (Operator next : root.getChildren()) {
				
				List<Operator> ninos = new ArrayList<>();				
				while (!(next instanceof Join) && !(next instanceof Merge) && next.getChildren().size() > 0) next = next.getChildren().get(0);
				ninos.addAll(getPermutatedOperatorsWithBlock(scope, next, joinPredConnections, joinFilterConnections, predicateConnections));
				
				if (ninos.isEmpty())  {
					hasPermutation = true;
					ninos.add(next);
				}
				
				combos.add(ninos);
			}
			
			if (!hasPermutation) {
				extraction.add(root);
				return extraction;
			}
			
			
			int[] positions = new int[4]; // counters for all children
			int totalCount = 1;
			for (int i = 0; i < combos.size() ; i++) totalCount *= combos.get(i).size(); // initialize

			int counter = 0;
			while (counter < totalCount) {
				
				// add a new instance
				Operator newAddition = root.duplicate(true); 
				extraction.add(newAddition);
				
				// modify each child individually
				for (int j = 0; j < combos.size() ; j ++) {
					
					Operator t = newAddition.getChildren().get(j); 
					
					// traverse to the nino location
					boolean usedAtLeastOnce = false;
					while (!(t instanceof Join) && !(t instanceof Merge) && t.getChildren().size() > 0) {
						t.getChildren().get(0).setParent(t);
						t = t.getChildren().get(0);
						usedAtLeastOnce = true;
					}
					Operator op = combos.get(j).get(positions[j]).duplicate(true);
					if (usedAtLeastOnce) {
						t = t.getParent();
						t.getChildren().clear();
						t.addChild(op);
						op.setParent(t);
					} else {
						newAddition.getChildren().set(j, op);
						op.setParent(newAddition);
					}
					
				}
				
				// advance the counter
				for (int j = 0; j < combos.size(); j++) 
					if (positions[j] >= combos.get(j).size()) {
						positions[j] = 0;
						if (j < combos.size() - 1) positions[j + 1] = positions[j + 1] + 1;
					}
				
				counter++;
			}
					
					
		} else {
			
			if (root.isPruned()) {
				return extraction;
			}
			
			// add all leaves and blockers to the lists, 
			
			List<Operator> treeWalker = root.getChildren();
			while (treeWalker.size() > 0) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator c : treeWalker) {
					
					// prune and block will never collide
					if (c.isPruned()) {
						
						leaves.add(c);
						
					} else if (c.isBlocking()) {
						
						Operator t = c.duplicate(true); // FULL REPLICATION
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
			List<Operator> permutationsOfLeaves = getPermutatedOperators(scope, leaves, joinPredConnections, joinFilterConnections, predicateConnections);
			
			// 2.
			Map<Integer, List<Operator>> blockerTrees = new HashMap<>();
			for (Operator b : blockers) {
				blockerTrees.put(b.getBlockerID(), getPermutatedOperatorsWithBlock(scope, b, joinPredConnections, joinFilterConnections, predicateConnections));
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
	
	private static boolean incrementStartingPoints(Integer id, Integer pos, Map<Integer, List<Operator>> blockerTrees, Map<Integer, Integer> startingPoints) {
		if (pos + 1 == blockerTrees.get(id).size()) {
			startingPoints.replace(id, 0);
			return true;
		} else {
			startingPoints.replace(id, pos + 1);
			return false;
		}
	}
	
	/**
	 * This function is only used for permuting the subtree of original remainder Operator tree
	 * where a blocking Operator is not involved.
	 * @param scope
	 * @param ops
	 * @param joinPredConnections
	 * @param joinFilterConnections
	 * @return A list of Operators that are each a root of a permuted remainder Operator tree.
	 * @throws Exception
	 */
	private static List<Operator> getPermutatedOperators(Scope scope, List<Operator> ops, Map<Pair<String, String>, String> joinPredConnections, Map<Pair<String, String>, String> joinFilterConnections, List<Set<String>> predicateConnections) throws Exception {
		
		List<Operator> extraction = new ArrayList<>();
		
		int len = ops.size();
		
		if (len == 1) {
			extraction.add(ops.get(0));
			return extraction;
			
		} else if (len == 2) {
			// the case of two
			extraction.add(makeJoin(scope, ops.get(0), ops.get(1), null, joinPredConnections, joinFilterConnections, new HashSet<>(), predicateConnections, true)); 
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
								addNewEntry(scope, k0o, k1o, joinPredConnections, joinFilterConnections, newEntries, predicateConnections);
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
								addNewEntry(scope, k0o, k1o, joinPredConnections, joinFilterConnections, newEntries, predicateConnections);
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
	 * Add new entry for partially constructed subtree permutation
	 * Note: use children characteristics to avoid making unnecessary entries.
	 * @param k0o
	 * @param k1o
	 * @param joinPredConnections
	 * @param joinFilterConnections
	 * @param newEntry
	 * @throws Exception
	 */
	private static void addNewEntry (Scope scope, Operator k0o, Operator k1o, Map<Pair<String, String>, String> joinPredConnections, Map<Pair<String, String>, String> joinFilterConnections, List<Operator> newEntry, List<Set<String>> predicateConnections) throws Exception {
		
		if (k1o instanceof Join) {
			// all on-expression must precede cross-joins
			if (k0o instanceof Join && (((Join)k0o).generateJoinPredicate() == null && ((Join)k1o).generateJoinPredicate() != null)) {
				return;
			} 
		
			if ( ((Join)k1o).generateJoinPredicate() != null || ((Join)k1o).generateJoinFilter() != null) {
				Set<String> objlist1 = new HashSet<>(k0o.getDataObjectAliasesOrNames().keySet());
				Set<String> objlist2 = new HashSet<>(objlist1);
				String jp = ((Join)k1o).generateJoinPredicate();
				String jf = ((Join)k1o).generateJoinFilter();
				
				boolean jpb = false;
				boolean jfb = false;
				
				if (jp != null) jpb = objlist1.removeAll( SQLExpressionUtils.getColumnTableNamesInAllForms(CCJSqlParserUtil.parseCondExpression(jp)) );
				if (jf != null) jfb = objlist2.removeAll( SQLExpressionUtils.getColumnTableNamesInAllForms(CCJSqlParserUtil.parseCondExpression(jf)) );
				
				if (jpb || jfb) { 
					newEntry.add(makeJoin(scope, k0o, k1o, null, joinPredConnections, joinFilterConnections, k0o.getDataObjectAliasesOrNames().keySet(), predicateConnections, true));
				}
			}
		} else {
			
			if ((k0o.isPruned() || k0o instanceof Scan) && (!k1o.isPruned() && !(k1o instanceof Scan))) {
				// pruned non-left-deep branch
				return;
			}
			
			Operator ret = makeJoin(scope, k0o, k1o, null, joinPredConnections,  joinFilterConnections, k0o.getDataObjectAliasesOrNames().keySet(), predicateConnections, true);
			if (ret == null) return; // the final prune done
			newEntry.add(ret);
		}
	}
	
	/**
	 * Create a Join Operator to connect two sub-trees or leaves of subtrees
	 * @param scope
	 * @param o1
	 * @param o2
	 * @param jt
	 * @param joinPredConnection
	 * @param joinFilterConnection
	 * @param used
	 * @return The constructed Join Operator
	 * @throws Exception
	 */
	private static Operator makeJoin(Scope scope, Operator o1, Operator o2, JoinType jt, 
			Map<Pair<String, String>, String> joinPredConnection, Map<Pair<String, String>, String> joinFilterConnection, 
			Set<String> used, List<Set<String>> predicateConnections, boolean isUsedByPermutation) throws Exception {
		
		Map<String, Map<String, String>> jp = new HashMap<>();
		Map<String, Map<String, String>> jf = new HashMap<>();
		
		for (Pair<String, String> p : joinPredConnection.keySet()) {
			if (!jp.containsKey(p.getLeft())) jp.put(p.getLeft(), new HashMap<>());
			jp.get(p.getLeft()).put(p.getRight(), joinPredConnection.get(p));
		}
		
		for (Pair<String, String> p : joinFilterConnection.keySet()) {
			if (!jf.containsKey(p.getLeft())) jf.put(p.getLeft(), new HashMap<>());
			jf.get(p.getLeft()).put(p.getRight(), joinFilterConnection.get(p));
		}
		
		Set<String> o1ns = new HashSet<>(o1.getDataObjectAliasesOrNames().keySet());
		Set<String> o2nsOriginal = new HashSet<>(o2.getDataObjectAliasesOrNames().keySet());
		Set<String> o2ns = new HashSet<>(o2nsOriginal);
		
		o1ns.retainAll(Sets.union(jp.keySet(), jf.keySet()));
		
//		System.out.printf("\n--->>>>> makeJoin: \n\tjp: %s; \n\tjf: %s;\n\to1ns: %s;\n\to2ns: %s\n\n", jp, jf, o1ns, o2ns);
		
		Operator o1Temp = o1;
		Operator o2Temp = o2;
		
		if (o1.isCopy()) o1Temp = (Join) o1.duplicate(true);
		if (o2.isCopy()) o2Temp = (Join) o2.duplicate(true);
		
		for (String s : o1ns) {
			
			if (jp.get(s) == null && jf.get(s) == null) continue; // because jc is modified along the way
			
			o2ns.retainAll(Sets.union(jp.keySet(), jf.keySet()));
			
			List<String> pred = new ArrayList<>();
			for  (String key : o2ns) {
				
//				System.out.printf("s: ; key: %s; used: %s\n", s, key, used);
				
				if (used.contains(key)) {
					continue;
				}
				
				boolean isFilter = false;
				
				if (jp.get(s) != null && jp.get(key) != null && jp.get(s).get(key) != null) {
					pred.add(jp.get(s).get(key));
					jp.get(s).remove(key);
					jp.get(key).remove(s);
				} else if (jf.get(s) != null && jf.get(key) != null && jf.get(s).get(key) != null) {
					pred.add(jf.get(s).get(key));
					jf.get(s).remove(key);
					jf.get(key).remove(s);
					isFilter = true;
				} else {
					continue;
				} 
				
				// this is the final checking of whether we need to prune it
				if (o1Temp instanceof Join && (!(o2Temp instanceof Join)) 
						&& (((Join) o1Temp).generateJoinPredicate() == null) && (((Join) o1Temp).generateJoinFilter() == null)) {
					
					if (isUsedByPermutation) return null;
					else break;
				}
//				System.out.printf("-------> jp: %s, s: %s, key: %s; pred: %s\n\n", jp, s, key, pred);
				return (new ArrayIsland()).constructJoin(o1Temp, o2Temp, jt, pred, isFilter);
			}
			
			o2ns = new HashSet<>(o2nsOriginal);
		}
		if (isTablesConnectedViaPredicates(o1Temp, o2Temp, predicateConnections) && isUsedByPermutation) {
			return null;
		} else {
			return (new ArrayIsland()).constructJoin(o1Temp, o2Temp, jt, null, false);
		}
	}
	
	/**
	 * Helper function that tests if two Operators have distinct sets of data
	 * This include pruned nodes and stored tables, etc.  
	 * @param s1
	 * @param s2
	 * @return
	 * @throws Exception
	 */
	private static boolean isDisjoint(Operator s1, Operator s2) throws Exception {
		Set<String> set1 = new HashSet<String>(s1.getDataObjectAliasesOrNames().keySet()); // s1.getDataObjectNames()
		Set<String> set2 = new HashSet<String>(s2.getDataObjectAliasesOrNames().keySet());
		return (!(set1.removeAll(set2)));
	}
	
	/**
	 * Pick out the pair of tables involved in a simple predicate. 
	 * Note: this assumes "a >= b" type of predicate that is devoid of logical operators such as "AND"
	 * @param jp
	 * @return
	 * @throws Exception
	 */
	private static Map<Pair<String, String>, String> processJoinPredicates(Set<String> jp) throws Exception {
		
		Map<Pair<String, String>, String> result = new HashMap<>();
		Map<Pair<String, String>, List<String>> intermediateResult = new HashMap<>();
		
		for (String s : jp ) {
			
			Expression e = CCJSqlParserUtil.parseCondExpression(s);
			List<Expression> le = SQLExpressionUtils.getFlatExpressions(e);
			
			for (Expression expr : le) {
				
				List<Column> lc = SQLExpressionUtils.getAttributes(expr);
				
				String temp0 = lc.get(0).getTable().getFullyQualifiedName(); // we don't look for other dots because everything is pruned
				String temp1 = lc.get(1).getTable().getFullyQualifiedName();
				
				Pair<String, String> temp01 = new ImmutablePair<>(temp0, temp1);
				Pair<String, String> temp10 = new ImmutablePair<>(temp1, temp0);
				
				if (intermediateResult.get(temp01) == null) intermediateResult.put(temp01, new ArrayList<>());
				if (intermediateResult.get(temp10) == null) intermediateResult.put(temp10, new ArrayList<>());
				intermediateResult.get(temp01).add(expr.toString());
				intermediateResult.get(temp10).add(expr.toString());
			}
		}
		
		for (Pair<String, String> pair : intermediateResult.keySet())
			result.put(pair, String.join(" AND ", intermediateResult.get(pair)));
			
		return result;
	}
	
	/**
	 * Determine which data sets are connected through a chain of Join predicates
	 * Note: this is used to avoid unnecessary CROSS JOINs
	 * @param jp
	 * @param jf
	 */
	private static List<Set<String>> populatePredicateConnectionSets(Map<Pair<String, String>, String> jp, Map<Pair<String, String>, String> jf) {
		
		SimpleGraph<String, DefaultEdge> sg = new SimpleGraph<>(DefaultEdge.class);
		for (Pair<String, String> pair : jp.keySet()) {
			if (!sg.containsVertex(pair.getLeft())) sg.addVertex(pair.getLeft());
			if (!sg.containsVertex(pair.getRight())) sg.addVertex(pair.getRight());
			sg.addEdge(pair.getLeft(), pair.getRight());
		}
		for (Pair<String, String> pair : jf.keySet()) {
			if (!sg.containsVertex(pair.getLeft())) sg.addVertex(pair.getLeft());
			if (!sg.containsVertex(pair.getRight())) sg.addVertex(pair.getRight());
			sg.addEdge(pair.getLeft(), pair.getRight());
		}
		return (new ConnectivityInspector<String, DefaultEdge>(sg)).connectedSets();
	}
	
	/**
	 * Determine if children of the two Operators (including themselves) are connected via a chain of predicates
	 * Note: this is used to avoid unnecessary CROSS JOINs 
	 * @param left
	 * @param right
	 * @return
	 * @throws Exception
	 */
	private static boolean isTablesConnectedViaPredicates(Operator left, Operator right, List<Set<String>> predicateConnections) throws Exception {
		Set<String> sl = left.getDataObjectAliasesOrNames().keySet();
		Set<String> sr = right.getDataObjectAliasesOrNames().keySet();
		
//		System.out.printf("\n-----------> table connectivity called; sl: %s; sr: %s; predicateConnections: %s;\n", sl, sr, predicateConnections);
		
		if (sr.removeAll(sl)) return true; 
		
		boolean found = false;
		for (Set<String> s : predicateConnections) {
			Set<String> scopy = new HashSet<>(s);
			if (scopy.removeAll(sl) && scopy.removeAll(sr)) {
				found = true;
				break;
			}
		}
		return found;
	}
	
	
	

}
package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import com.google.common.collect.Sets;

import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.relational.operators.SQLIslandAggregate;
import istc.bigdawg.islands.relational.operators.SQLIslandJoin;
import istc.bigdawg.islands.relational.operators.SQLIslandOperator;
import istc.bigdawg.islands.relational.operators.SQLIslandScan;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

public class CrossIslandQueryNodes {
	
	public static void optimize(CrossIslandQueryNode ciqn) throws Exception {
		rewrite(ciqn);
		permute(ciqn);
	}
	
	/**
	 * The rewrite is a set of optimizations that occurs after the pruning. 
	 * @param ciqn
	 * @throws Exception
	 */
	public static void rewrite(CrossIslandQueryNode ciqn) throws Exception{
//		trimSchemas(ciqn);
		if (ciqn.getRemainderLoc() != null) return;
		
//		debugPrinting(ciqn);
//		minimumMigrationMutation(ciqn);
		debugPrinting(ciqn);
	}
	
	private static void debugPrinting(CrossIslandQueryNode ciqn) throws Exception {
		
		System.out.println("\nRemainder Printing: ");
		debugPrintOperator(ciqn.getInitialRoot(), 0);
		
		for (String s : ciqn.getQueryContainer().keySet()) {
			System.out.printf("\nContainer %s: \n", s);
			debugPrintOperator(ciqn.getQueryContainer().get(s).getRootOperator(), 0);
		}
		
	}
	
	private static void debugPrintOperator(Operator o, int indent) throws Exception {
		for (int i = 1; i <= indent; i++) System.out.print('\t');
		System.out.printf("%s %s\n", o.getClass().getSimpleName(), o.isPruned() ? o.getPruneToken() : (o instanceof Scan ? ((Scan)o).getSourceTableName() : o.getSubTreeToken()));
		for (Operator c : o.getChildren()) debugPrintOperator(c, indent+1);
	} 
	
	private static void trimSchemas(CrossIslandQueryNode ciqn) throws Exception {
		trimSchemaForSQL(ciqn.getInitialRoot(), ciqn.getInitialRoot().getOutSchema().values());
	}
	
	private static void trimSchemaForSQL(Operator operator, Collection<DataObjectAttribute> mentionedAttributes) throws JSQLParserException, Exception {
		Set<String> end_result = new HashSet<>();
		
		System.out.printf("\ntrimSchemaForSQL; mentioendAttributes = %s;\n", mentionedAttributes);
		
		for (String name : operator.getOutSchema().keySet()) {
			
			DataObjectAttribute doa = operator.getOutSchema().get(name);
			String doaname = CCJSqlParserUtil.parseExpression(doa.name).toString();
			
			System.out.printf("trimSchemaForSQL; doa.name = %s;\n", doa.name);
			
			if (mentionedAttributes.contains(doa)) 
				continue;
			else if (operator instanceof Join) {
				SQLIslandJoin j = (SQLIslandJoin) operator;
				if (j.generateJoinFilter() != null && j.generateJoinFilter().contains(doaname)) continue;
				if (j.generateJoinPredicate() != null && j.generateJoinPredicate().contains(doaname)) continue;
			} else if (operator instanceof Scan) {
				SQLIslandScan s = (SQLIslandScan) operator;
				if (s.getFilterExpression() != null && s.getFilterExpression().toString().contains(doaname)) continue;
				if (s.getIndexCond() != null && s.getIndexCond().toString().contains(doaname)) continue;
			} else if (operator instanceof Aggregate) {
				SQLIslandAggregate a = (SQLIslandAggregate) operator;
				if (a.getAggregateFilter() != null && a.getAggregateFilter().contains(doaname)) continue;
			}
			end_result.add(name);
		}
		System.out.printf("trimSchemaForSQL; end_result = %s; original = %s\n", end_result, operator.getOutSchema().keySet());
		
		for (String s : end_result)
			operator.getOutSchema().remove(s);
		for (Operator o : operator.getChildren()) {
			trimSchemaForSQL(o, mentionedAttributes);
		}
	}
	
	private static void minimumMigrationMutation(CrossIslandQueryNode ciqn) throws Exception {
		
		Operator initialRemainder = ciqn.getInitialRoot();
		Map<String, DataObjectAttribute> initialOutSchema = initialRemainder.getOutSchema();
		Map<String, QueryContainerForCommonDatabase> containerStarters = ciqn.getQueryContainer();
		Map<String, QueryContainerForCommonDatabase> resultContainers = new HashMap<>();
		
		Map<Pair<String, String>, String> jp = processJoinPredicates(ciqn.getJoinPredicates()); 
		Map<Pair<String, String>, String> jf = processJoinPredicates(ciqn.getJoinFilters());
		List<Set<String>> predicateConnections = populatePredicateConnectionSets(jp, jf);
		
		/*
		 *  scrape for the whole reachable layer to see if anything could be combined
		 *  - if no, then enqueue each blocker for scanning, and collect each container involved
		 *  - otherwise, extract the subtree root schema, make a few joins, then re-apply the schema
		 *  Repeat until there is none left in the processing queue
		 */
		
		// data structure for the main loop flow
		Stack<Operator> processQueue = new Stack<>();
		Set<Operator> currentPrunedSet = new HashSet<>();

		// for rewriting the subtree
		List<Operator> currentLeavesSet = new ArrayList<>();
		Operator lastStop = null;
		
		// initialize
		processQueue.add(initialRemainder);
		
		/*
		 * MAIN LOOP
		 * Want: 
		 * for each blocked, mark as a root to traverse and keep going 
		 * for each join, expand all you can 
		 */
		while (!processQueue.isEmpty()) {
			
			Operator o = processQueue.pop();
			
			if (o.isBlocking()) {
				
				lastStop = o;
				
				for (Operator c : o.getChildren()) {
					if (c.isBlocking() ) {
						// add to the back
						processQueue.add(o.getChildren().get(0));
					} else if ( c instanceof Join) {
						// push to the front
						processQueue.push(c);
					} else 
						throw new BigDawgException ("shouldn't be here; "+o.getChildren().get(0).getClass().getSimpleName()
													+"; isPruned: "+o.getChildren().get(0).isPruned());
				}
				continue;
					
			} else if (o instanceof Join) {
				
				normalizeCurrentOperator(o);
				
				// it is a join unpruned
				Stack<Operator> tempStack = new Stack<>();
				tempStack.add(o);
				
				while (!tempStack.isEmpty()) {
					Operator t = tempStack.pop();
					for (Operator c : t.getChildren()) 
						if (c.isPruned()) {
							currentPrunedSet.add(c);
						} else if (c.isBlocking()) {
							processQueue.add(c);
							currentLeavesSet.add(o.getChildren().get(0));
						} else // c is instanceof unpruned Join
							tempStack.push(c);
				}
			} else 
				throw new BigDawgException ("Impossible scenario: "+o.getClass().getSimpleName()+"; isPruned: "+o.isPruned());
			
			
			// batch done -- this is only reached if o is Join
			
			// grouping
			Map<String, List<QueryContainerForCommonDatabase>> locationToContainer = new HashMap<>();
			for (Operator p : currentPrunedSet) {
				QueryContainerForCommonDatabase con = containerStarters.get(p.getPruneToken());
				if (locationToContainer.get(con.getDBID()) == null) locationToContainer.put(con.getDBID(), new ArrayList<>());
				locationToContainer.get(con.getDBID()).add(con);
			}
			
			// combining
			for (String dbid : locationToContainer.keySet()) {
				QueryContainerForCommonDatabase con;
				if (locationToContainer.get(dbid).size() > 1) 
					con = mergeContainers(ciqn.getSourceScope(), locationToContainer.get(dbid), jp, jf, predicateConnections);
				else 
					con = locationToContainer.get(dbid).get(0);
				resultContainers.put(con.getName(), con);
				currentLeavesSet.add(con.getRootOperator());
			}
			
			// restructuring 
			Operator a = currentLeavesSet.remove(0);
			while (!currentLeavesSet.isEmpty()) {
				Operator d = currentLeavesSet.remove(0);
				if (isNormalizationRequired(a, d)) {
					Operator temp = a;
					a = d;
					d = temp;
				}
				
				a = makeJoin(ciqn.getSourceScope(), a, d, null, jp, jf, a.getDataObjectAliasesOrNames().keySet(), predicateConnections, false);
			}
			if (lastStop != null) {
				a.setParent(lastStop);
				lastStop.getChildren().clear();
				lastStop.addChild(a);
			} else {
				initialRemainder = a;
				initialRemainder.getOutSchema().clear();
				initialRemainder.getOutSchema().putAll(initialOutSchema);
			}
			
			// resetting after a Join head expansion run
			currentPrunedSet = new HashSet<>();
			currentLeavesSet = new ArrayList<>();
		}

		// finished mutation; add the winning set
		ciqn.setInitialRoot(initialRemainder);
		ciqn.setQueryContainer(resultContainers);
	}
	
	private static QueryContainerForCommonDatabase mergeContainers(Scope scope, List<QueryContainerForCommonDatabase> containers, 
			Map<Pair<String, String>, String> joinPredConnection, Map<Pair<String, String>, String> joinFilterConnection, 
			List<Set<String>> predicateConnections) throws Exception {
		
		QueryContainerForCommonDatabase con1 = containers.remove(0);  // all of them share the same DBID and therefore the same CI
		Operator o1 = con1.getRootOperator();
		o1.prune(false);
		while (!containers.isEmpty()) {
			Operator o2 = containers.remove(0).getRootOperator();
			o2.prune(false);
			if (isNormalizationRequired(o1, o2)) {
				Operator temp = o1;
				o1 = o2;
				o2 = temp;
			}
			o1 = makeJoin(scope, o1, o2, null, joinPredConnection, joinFilterConnection, o1.getDataObjectAliasesOrNames().keySet(), predicateConnections, false);
		}
		o1.prune(true);
		return new QueryContainerForCommonDatabase(con1.getConnectionInfo(), con1.getDBID(), o1, o1.getPruneToken());
	}
	
	private static void normalizeCurrentOperator(Operator o) {
		if (!(o instanceof Join)) return;
		
		if (isNormalizationRequired(o.getChildren().get(0), o.getChildren().get(1))){ 
			Operator temp = o.getChildren().get(0);
			o.getChildren().remove(0);
			o.addChild(temp);
		};
	} 
	
	private static boolean isNormalizationRequired(Operator o1, Operator o2) {
		return (o2 instanceof Join && !o2.isPruned() && o1.isPruned() || !(o1 instanceof Join));
	}
	
	public static void permute(CrossIslandQueryNode ciqn) throws Exception {
		
		List<Operator> remainderPermutations = new ArrayList<>();
		
		Map<Pair<String, String>, String> jp = processJoinPredicates(ciqn.getJoinPredicates());
		Map<Pair<String, String>, String> jf = processJoinPredicates(ciqn.getJoinFilters());
		
		List<Set<String>> predicateConnections = populatePredicateConnectionSets(jp, jf);
		Operator root = ciqn.getInitialRoot(); 
		
		if (ciqn.getRemainderLoc() == null && root.getDataObjectAliasesOrNames().size() > 1) {
			Map<String, DataObjectAttribute> rootOutSchema = root.getOutSchema();
			List<Operator> permResult = getPermutatedOperatorsWithBlock(ciqn.getSourceScope(), root, jp, jf, predicateConnections);
			
			// if root is join then the constructed out schema might get messed up; adjust it here
			if (root instanceof SQLIslandOperator) {
				for (Operator op : permResult) {
					SQLIslandOperator o = (SQLIslandOperator) op;
					if (o.getOutSchema().size() != rootOutSchema.size()) {
						o.updateOutSchema(rootOutSchema);
					}
				}
			}
			
//			// for debugging
//			System.out.println("\n\n\nResult of Permutation: ");
//			int i = 1;
//			OperatorQueryGenerator gen;
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
//			OperatorQueryGenerator gen = null;
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
//				return TheObjectThatResolvesAllDifferencesAmongTheIslands.constructJoin(scope, o1Temp, o2Temp, jt, pred, isFilter);
				return TheObjectThatResolvesAllDifferencesAmongTheIslands.getIsland(scope).constructJoin(o1Temp, o2Temp, jt, pred, isFilter);
			}
			
			o2ns = new HashSet<>(o2nsOriginal);
		}
		if (isTablesConnectedViaPredicates(o1Temp, o2Temp, predicateConnections) && isUsedByPermutation) {
			return null;
		} else {
//			System.out.printf("\nCross island query node: raw cross join:\n  o1 class: %s; o1Temp tree: %s;\n  o2 class: %s, o2Temp tree: %s\n\n\n"
//					, o1Temp.getClass().getSimpleName(), o1Temp.getTreeRepresentation(true)
//					, o2Temp.getClass().getSimpleName(), o2Temp.getTreeRepresentation(true));
//			return TheObjectThatResolvesAllDifferencesAmongTheIslands.constructJoin(scope, o1Temp, o2Temp, jt, null, false);
			return TheObjectThatResolvesAllDifferencesAmongTheIslands.getIsland(scope).constructJoin(o1Temp, o2Temp, jt, null, false);
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

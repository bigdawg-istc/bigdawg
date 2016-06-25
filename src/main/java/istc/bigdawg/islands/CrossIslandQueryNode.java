package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import com.google.common.collect.Sets;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.PostgreSQL.SQLPlanParser;
import istc.bigdawg.islands.PostgreSQL.SQLQueryGenerator;
import istc.bigdawg.islands.PostgreSQL.SQLQueryPlan;
import istc.bigdawg.islands.PostgreSQL.operators.PostgreSQLIslandJoin;
import istc.bigdawg.islands.PostgreSQL.operators.PostgreSQLIslandOperator;
import istc.bigdawg.islands.PostgreSQL.operators.PostgreSQLIslandScan;
import istc.bigdawg.islands.PostgreSQL.utils.SQLExpressionUtils;
import istc.bigdawg.islands.SciDB.AFLPlanParser;
import istc.bigdawg.islands.SciDB.AFLQueryGenerator;
import istc.bigdawg.islands.SciDB.AFLQueryPlan;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandJoin;
import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Limit;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.islands.operators.Sort;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.signature.builder.ArraySignatureBuilder;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Select;

public class CrossIslandQueryNode extends CrossIslandPlanNode {
	
	private Select select;
	private Signature signature;
	private DBHandler dbSchemaHandler = null;
	
	private Map<String, QueryContainerForCommonDatabase> queryContainer;
	private List<Operator> remainderPermutations;
	private List<String> remainderLoc; // if null then we need to look into the container
	
	private Map<String, List<String>> originalMap;
	private Set<String> children;
//	private Matcher tagMatcher;
	
	private Set<String> originalJoinPredicates;
	private Set<String> joinPredicates;
	private Set<String> joinFilters;
//	private Set<String> scansWithIndexCond;
	private List<Set<String>> predicateConnections;
//	private Map<Set<Integer>, Object> outSchemaByPrunes; // populated during traverse(), used when constructing
	
	private static final int  psqlSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	private static final int  scidbSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getSciDBSchemaServerDBID();
	private static final String tokenOfIndecision = "-1";
	
	public CrossIslandQueryNode (IslandsAndCast.Scope scope, String islandQuery, String name, Map<String, String> transitionSchemas) throws Exception {
		super(scope, islandQuery, name);
		
		queryContainer = new HashMap<>();
		remainderPermutations = new ArrayList<>();
		remainderLoc = new ArrayList<>();
		joinPredicates = new HashSet<>();
		joinFilters = new HashSet<>();
		originalJoinPredicates = new HashSet<>();
//		outSchemaByPrunes = new HashMap<>();
		
//		// collect the cross island children
		children = getCrossIslandChildrenReferences(transitionSchemas);
		
		System.out.printf("\n-> Island query: %s;\n", islandQuery);
		System.out.printf("---> CrossIslandChildren: %s;\n",children.toString());
		System.out.printf("---> TransitionSchemas: %s;\n\n",transitionSchemas.toString());
		
		// create temporary tables that are used for as schemas
		createTableForPlanning(transitionSchemas);
		
		populateQueryContainer(scope, transitionSchemas);
		this.signature = new Signature(getQueryString(), getSourceScope(), getRemainder(0), getQueryContainer(), originalJoinPredicates);
		
		// removing temporary schema plates
		removeTemporaryTableCreatedForPlanning(transitionSchemas);
		
		
	}
	
//	private void createTableForPlanning(Map<String, Operator> rootsForSchemas) throws Exception {
	private void createTableForPlanning(Map<String, String> transitionSchemas) throws Exception {

		// NEW METHOD
		
		if (sourceScope.equals(Scope.RELATIONAL)) {
			dbSchemaHandler = new PostgreSQLHandler((PostgreSQLConnectionInfo)CatalogViewer.getConnectionInfo(psqlSchemaHandlerDBID));
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) {
					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL(transitionSchemas.get(key));
				}
		} else if (sourceScope.equals(Scope.ARRAY)) {
			dbSchemaHandler = new SciDBHandler((SciDBConnectionInfo)CatalogViewer.getConnectionInfo(scidbSchemaHandlerDBID));
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) {
					((SciDBHandler)dbSchemaHandler).executeStatement(transitionSchemas.get(key));
					((SciDBHandler)dbSchemaHandler).commit();
				}
		} else
			throw new Exception("Unsupported island code : "+sourceScope.toString());
		// NEW METHOD END
		
//		OperatorVisitor gen = null;
//		
//		// create new tables or arrays for planning use
//		if (sourceScope.equals(Scope.RELATIONAL)) {
//			this.select = (Select) CCJSqlParserUtil.parse(queryString);
//			dbSchemaHandler = new PostgreSQLHandler(psqlSchemaHandlerDBID);
//			gen = new SQLQueryGenerator();
//			for (String key : transitionSchemas.keySet()) {
//				if (children.contains(key)) {
//					System.out.println("key: "+key+"; query: "+gen.generateCreateStatementLocally(transitionSchemas.get(key), key)+"\n\n");
//					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL(gen.generateCreateStatementLocally(transitionSchemas.get(key), key));
//				}
//			}
//		} else if (sourceScope.equals(Scope.ARRAY)) {
//			dbSchemaHandler = new SciDBHandler(scidbSchemaHandlerDBID);
//			gen = new AFLQueryGenerator();
//			for (String key : transitionSchemas.keySet()) {
//				if (children.contains(key)) {
//					System.out.println("key: "+key+"; query: "+gen.generateCreateStatementLocally(transitionSchemas.get(key), key)+"\n\n");
//					((SciDBHandler)dbSchemaHandler).executeStatement(gen.generateCreateStatementLocally(transitionSchemas.get(key), key));
//				}
//			}
//		} else
//			throw new Exception("Unsupported island code : "+sourceScope.toString());
	}
	
//	private void removeTemporaryTableCreatedForPlanning(Map<String, Operator> rootsForSchemas) throws Exception {
	private void removeTemporaryTableCreatedForPlanning(Map<String, String> transitionSchemas) throws Exception {
		if (sourceScope.equals(Scope.RELATIONAL)) {
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) 
					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL("drop table "+key);
		} else if (sourceScope.equals(Scope.ARRAY)) {
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) {
					((SciDBHandler)dbSchemaHandler).executeStatementAFL("remove("+key+")");
					((SciDBHandler)dbSchemaHandler).commit();
				}
		} else
			throw new Exception("Unsupported island code : "+sourceScope.toString());
	}
	

	public Set<String> getCrossIslandChildrenReferences(Map<String, String> transitionSchemas) {

		// NEW METHOD
		Set<String> offsprings = new HashSet<>(transitionSchemas.keySet());
		return offsprings;
		// NEW METHOD END
		
//		// aka find children
//		
//		tagMatcher = Pattern.compile("\\bBIGDAWGTAG_[0-9]+\\b").matcher(queryString);
//		Set<String> offsprings = new HashSet<>();
//		
//		while (tagMatcher.find()) {
//			offsprings.add(queryString.substring(tagMatcher.start(), tagMatcher.end()));
//		}
//		
//		return offsprings;
	}
	
	public QueryExecutionPlan getQEP(int perm, boolean isSelect) throws Exception {
		
		
		
		// use perm to pick a specific permutation
		if (perm >= remainderPermutations.size()) throw new Exception ("Permutation reference index out of bound");
		
		QueryExecutionPlan qep = new QueryExecutionPlan(sourceScope); 
		ExecutionNodeFactory.addNodesAndEdges(qep, remainderPermutations.get(perm), remainderLoc, queryContainer, isSelect, name);
//		ExecutionNodeFactory.addNodesAndEdgesNaive( qep, remainderPermutations.get(perm), remainderLoc, queryContainer);
		
		return qep;
	}
	
	public List<QueryExecutionPlan> getAllQEPs(boolean isSelect) throws Exception {
		
		List<QueryExecutionPlan> qepl = new ArrayList<>();
		
		System.out.printf("RemainderPermuations, from getAllQEPs: %s\n", remainderPermutations);
		
		for (int i = 0; i < remainderPermutations.size(); i++ ){
			QueryExecutionPlan qep = new QueryExecutionPlan(getSourceScope()); 
			ExecutionNodeFactory.addNodesAndEdges(qep, remainderPermutations.get(i), remainderLoc, queryContainer, isSelect, name);
			qepl.add(qep);
		}
		
		return qepl;
	}
	
	/** Setup. 
	 * 
	 * @throws Exception
	 */
	private void populateQueryContainer(Scope scope, Map<String, String> transitionSchemas) throws Exception {
		
		// NOW WE ONLY SUPPORT RELATIONAL ISLAND
		// SUPPORT OTHER ISLANDS && ISLAND CHECK 
		
		Operator root = null;
		List<String> objs = null;
		
		System.out.println("Original query to be parsed: \n"+queryString);
		
		if (getSourceScope().equals(Scope.RELATIONAL)) {
			SQLQueryPlan queryPlan = SQLPlanParser.extractDirect((PostgreSQLHandler)dbSchemaHandler, queryString);
			root = queryPlan.getRootNode();
			objs = new ArrayList<>(RelationalSignatureBuilder.sig2(queryString));
		} else if (getSourceScope().equals(Scope.ARRAY)) {
			AFLQueryPlan queryPlan = AFLPlanParser.extractDirect((SciDBHandler)dbSchemaHandler, queryString);
			root = queryPlan.getRootNode();
			objs = new ArrayList<>(ArraySignatureBuilder.sig2(queryString));
		} else 
			throw new Exception("Unsupported island code: "+getSourceScope().toString());

		originalJoinPredicates.addAll(getOriginalJoinPredicates(root));
		originalMap = CatalogViewer.getDBMappingByObj(objs, getSourceScope());
		
//		System.out.printf("----> printing original map from Populate: %s; objs: %s; scope: %s\n", originalMap, objs, getSourceScope().toString());
		
		// traverse add remainder
		if (root instanceof PostgreSQLIslandOperator) System.out.printf("\n\n\n--> Root schema before traverse: %s;\n", ((PostgreSQLIslandOperator)root).getOutSchema());
		
		remainderLoc = traverse(root, transitionSchemas); // this populated everything
		
		System.out.printf("----> resulting remainder loc: %s\n", remainderLoc);
		
		Map<Pair<String, String>, String> jp = processJoinPredicates(joinPredicates);
		Map<Pair<String, String>, String> jf = processJoinPredicates(joinFilters);
		
		populatePredicateConnectionSets(jp, jf);
		
		
		if (remainderLoc == null && root.getDataObjectAliasesOrNames().size() > 1) {
			Map<String, DataObjectAttribute> rootOutSchema = ((PostgreSQLIslandOperator) root).getOutSchema();
			List<Operator> permResult = getPermutatedOperatorsWithBlock(scope, root, jp, jf);
			// if root is join then the constructed out schema might get messed up
			System.out.printf("--> Root schema before if: %s;\n", ((PostgreSQLIslandOperator)root).getOutSchema());
			if (root instanceof PostgreSQLIslandOperator) {
				
				for (Operator op : permResult) {
					PostgreSQLIslandOperator o = (PostgreSQLIslandOperator) op;
					if (o.getOutSchema().size() != rootOutSchema.size()) {
						System.out.printf("--> o schema before: %s;\n", o.getOutSchema());
						o.updateOutSchema(rootOutSchema);
						System.out.printf("--> o schema after update: %s;\n\n", o.getOutSchema());
					}
				}
			}
			
			// debug
			System.out.println("\n\n\nResult of Permutation: ");
			int i = 1;
			OperatorVisitor gen;
			for (Operator o : permResult) {
				if (getSourceScope().equals(Scope.RELATIONAL)) {
					gen = new SQLQueryGenerator();
				} else if (getSourceScope().equals(Scope.ARRAY)) {
					gen = new AFLQueryGenerator();
				} else gen = null;
				gen.configure(true, false);
				o.accept(gen);
				System.out.printf("%d.  first formulation: %s\n", i, gen.generateStatementString());
				System.out.printf("%d. second formulation: %s\n", i, gen.generateStatementString()); // duplicate command to test modification of underlying structure
				System.out.printf("--> o schema after gens: %s;\n\n", ((PostgreSQLIslandOperator) o).getOutSchema());
				i++;
			}
			System.out.println("\n");
			// end of debug
			
//			remainderPermutations.clear();
			remainderPermutations.addAll(permResult);
			
//			// debug
//			remainderPermutations.add(root);
			
		} // if remainderLoc is not null, then THERE IS NO NEED FOR PERMUTATIONS 
		else {
			remainderPermutations.add(root);
		}
		
	}

	private Set<String> getOriginalJoinPredicates(Operator root) throws Exception{
		Set<String> predicates = new HashSet<>();

		if (root == null){
			return predicates;
		}

		if (root instanceof Join){
			String predicate = ((Join) root).generateJoinPredicate();
			if (predicate != null){
				predicates.addAll(splitPredicates(predicate));
			}

			predicate = ((Join) root).generateJoinFilter();
			if (predicate != null){
				predicates.addAll(splitPredicates(predicate));
			}

		} else if (root instanceof  Scan){
			String predicate = ((Scan) root).generateRelevantJoinPredicate();
			if (predicate != null){
				predicates.addAll(splitPredicates(predicate));
			}
		}

		for (Operator child: root.getChildren()){
			predicates.addAll(getOriginalJoinPredicates(child));
		}

//		System.out.printf("\n\n\n---------> all predicates of %s: %s\n\n\n\n", root.getClass().getSimpleName(), predicates);
		return predicates;
	}

	private Set<String> splitPredicates(String predicates){
		Set<String> results = new HashSet<>();
		Pattern predicatePattern = Pattern.compile("(?<=\\()([^\\(^\\)]+)(?=\\))");

		String joinDelim = "";
		if (getSourceScope().equals(Scope.RELATIONAL)){
			joinDelim = "=";
		} else if (getSourceScope().equals(Scope.ARRAY)){
			// TODO ensure this is correct for SciDB
			joinDelim = ",";
		}

		Matcher m = predicatePattern.matcher(predicates);
		while (m.find()){
			String current = m.group().replace(" ", "");
			String[] filters = current.split(joinDelim);
			Arrays.sort(filters);
			String result = String.join(joinDelim, filters);
			results.add(result);
		}
		return results;
	}
	
	
	private List<Operator> getPermutatedOperatorsWithBlock(Scope scope, Operator root, Map<Pair<String, String>, String> joinPredConnections,  Map<Pair<String, String>, String> joinFilterConnections) throws Exception {
		
		/**
		 * This function dictates which part of the tree could be permuted.
		 * Note, only Join and Scan type of operators are NOT blocking. 
		 */
		
		List<Operator> extraction = new ArrayList<>();
		List<Operator> blockers   = new ArrayList<>();
		List<Operator> leaves 	  = new ArrayList<>();

		
		if (root.isBlocking() ) { //&& !(root instanceof Merge)) {
			// DEBUG ONLY
			OperatorVisitor gen = null;
			if (getSourceScope().equals(Scope.RELATIONAL)) {
				gen = new SQLQueryGenerator();
				((SQLQueryGenerator)gen).setSrcStatement(select);
			}
			gen.configure(true, false);
			root.accept(gen);
			
			System.out.println("--> blocking root; class: "+root.getClass().getSimpleName()+"; ");
			System.out.println("--> tree rep: "+root.getTreeRepresentation(true)+"; ");
			System.out.println("--> SQL: "+gen.generateStatementString()+"; \n");
			// DEBUG OUTPUT END
			
			
			boolean hasPermutation = false;
			List<List<Operator>> combos = new ArrayList<>();	// holder of operators
			for (Operator next : root.getChildren()) {
				
				List<Operator> ninos = new ArrayList<>();				
				while (!(next instanceof Join) && !(next instanceof Merge) && next.getChildren().size() > 0) next = next.getChildren().get(0);
				ninos.addAll(getPermutatedOperatorsWithBlock(scope, next, joinPredConnections, joinFilterConnections));
				
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
					
					
//				}
//				// advance counters for all children 
//				positions.set(comboCount, positions.get(comboCount) + 1);
//				for (int i = combos.size() - 1; i >= 0 ; i--) 
//					if (positions.get(i) >= combos.get(i).size()) {
//						positions.set(i, 0);
//						if (i > 0) positions.set(i - 1, positions.get(i - 1) + 1);
//					}
//			}
//			
//			for (Operator o: ninos) {
//				
//				Operator t = root.duplicate(true); // FULL REPLICATION
//
//				extraction.add(t);
//				
//				for (Operator op : t.getChildren()) op.setParent(t);
//				
//				while (!(t instanceof Join) && !(t instanceof Merge)) {
//					t = t.getChildren().get(0);
//					t.getChildren().get(0).setParent(t);
//				}
////				if (t.getParent() == null) continue;
//				t = t.getParent();
//				t.getChildren().clear();
//				t.addChild(o);
//			}
			
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
			List<Operator> permutationsOfLeaves = getPermutatedOperators(scope, leaves, joinPredConnections, joinFilterConnections);
			Map<Integer, List<Operator>> blockerTrees = new HashMap<>();
			
			// 2.
			for (Operator b : blockers) {
				blockerTrees.put(b.getBlockerID(), getPermutatedOperatorsWithBlock(scope, b, joinPredConnections, joinFilterConnections));
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
	
	
	private List<Operator> getPermutatedOperators(Scope scope, List<Operator> ops, Map<Pair<String, String>, String> joinPredConnections, Map<Pair<String, String>, String> joinFilterConnections) throws Exception {
		
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
			extraction.add(makeJoin(scope, ops.get(0), ops.get(1), null, joinPredConnections, joinFilterConnections, new HashSet<>())); 
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
								
								addNewEntry(scope, k0o, k1o, joinPredConnections, joinFilterConnections, newEntries);
								 
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
								addNewEntry(scope, k0o, k1o, joinPredConnections, joinFilterConnections, newEntries);
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
	private void addNewEntry (Scope scope, Operator k0o, Operator k1o, Map<Pair<String, String>, String> joinPredConnections, Map<Pair<String, String>, String> joinFilterConnections, List<Operator> newEntry) throws Exception {
		
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
					newEntry.add(makeJoin(scope, k0o, k1o, null, joinPredConnections, joinFilterConnections, k0o.getDataObjectAliasesOrNames().keySet()));
				}
			}
		} else {
			
			if ((k0o.isPruned() || k0o instanceof Scan) && (!k1o.isPruned() && !(k1o instanceof Scan))) {
				// pruned non-left-deep branch
				return;
			}
			
			Operator ret = makeJoin(scope, k0o, k1o, null, joinPredConnections,  joinFilterConnections, k0o.getDataObjectAliasesOrNames().keySet());
			if (ret == null) return; // the final prune done
			newEntry.add(ret);
		}
	}
	
	private Operator makeJoin(Scope scope, Operator o1, Operator o2, JoinType jt, Map<Pair<String, String>, String> joinPredConnection, Map<Pair<String, String>, String> joinFilterConnection, Set<String> used) throws Exception {
		
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
		
		
		
//		for (String k : joinFilterConnection.keySet()) {
//			jf.put(k, new HashMap<>());
//			for (String kin : joinFilterConnection.get(k).keySet()) {
//				jf.get(k).put(kin, joinFilterConnection.get(k).get(kin));
//			}
//		}
		
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
			
			if (jp.get(s) == null) continue; // because jc is modified along the way
			
			o2ns.retainAll(Sets.union(jp.keySet(), jf.keySet()));
			
			
			List<String> pred = new ArrayList<>();
			for  (String key : o2ns) {
				
				while (used.contains(key)) {
					continue;
				}
				
				boolean isFilter = false;
				
				if (jp.get(s) != null && jp.get(key) != null && jp.get(s).get(key) != null) {
					pred.add(jp.get(s).get(key));
//					System.out.printf("---------> jp: %s, s: %s, key: %s; pred: %s; used: %s\n", jp, s, key, pred, used);
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
					
					return null;
				}
				System.out.printf("-------> jp: %s, s: %s, key: %s; pred: %s\n\n", jp, s, key, pred);
				return constructJoin(scope, o1Temp, o2Temp, jt, String.join(" AND ", pred), isFilter);
			}
			
			o2ns = new HashSet<>(o2nsOriginal);
		}
		if (isTablesConnectedViaPredicates(o1Temp, o2Temp)) {
			return null;
		} else {
			System.out.printf("\nCross island query node: raw cross join:\n  o1 class: %s; o1Temp tree: %s;\no  2 class: %s, o2Temp tree: %s\n\n\n"
					, o1Temp.getClass().getSimpleName(), o1Temp.getTreeRepresentation(true)
					, o2Temp.getClass().getSimpleName(), o2Temp.getTreeRepresentation(true));
			return constructJoin(scope, o1Temp, o2Temp, jt, null, false);
		}
	}
	
	private Join constructJoin (Scope scope, Operator o1, Operator o2, JoinType jt, String joinPred, boolean isFilter) throws Exception {
		if (scope.equals(Scope.RELATIONAL))
			return new PostgreSQLIslandJoin(o1, o2, jt, joinPred, isFilter);
		else if (scope.equals(Scope.ARRAY))
			return new SciDBIslandJoin().construct(o1, o2, jt, joinPred, isFilter);
		else 
			throw new Exception("Unimplemented island: "+scope.name());
	}
	
	private boolean isDisjoint(Operator s1, Operator s2) throws Exception {
		Set<String> set1 = new HashSet<String>(s1.getDataObjectAliasesOrNames().keySet()); // s1.getDataObjectNames()
		Set<String> set2 = new HashSet<String>(s2.getDataObjectAliasesOrNames().keySet());
		return (!(set1.removeAll(set2)));
	}
	
	
	private Map<Pair<String, String>, String> processJoinPredicates(Set<String> jp) throws Exception {
		
		Map<Pair<String, String>, String> result = new HashMap<>();
		
		for (String s : jp ) {
			
			Expression e = CCJSqlParserUtil.parseCondExpression(s);
			List<Expression> le = SQLExpressionUtils.getFlatExpressions(e);
			for (Expression expr : le) {
				
				List<Column> lc = SQLExpressionUtils.getAttributes(expr);
				
				String temp0 = lc.get(0).getTable().getFullyQualifiedName(); // we don't look for other dots because everything is pruned
				String temp1 = lc.get(1).getTable().getFullyQualifiedName();
				result.put(new ImmutablePair<>(temp0, temp1), s);
				result.put(new ImmutablePair<>(temp1, temp0), s);
			}
		}
			
		return result;
	}
	
	
	private List<String> traverse(Operator node, Map<String, String> transitionSchemas) throws Exception, Exception {
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
			
			if ( transitionSchemas.containsKey(((SeqScan) node).getFullyQualifiedName())){
				
				System.out.print("--> transitionSchema marked\n");
				
				ret = new ArrayList<String>();
				if (getSourceScope().equals(Scope.RELATIONAL))
					ret.add(String.valueOf(psqlSchemaHandlerDBID));						// IMPORTANT. CHANGE ORIGINAL MAPPING TO INCLUDE THIS
				else if (getSourceScope().equals(Scope.ARRAY))
					ret.add(String.valueOf(scidbSchemaHandlerDBID));
				else 
					throw new Exception("Unsupported island: "+getSourceScope().name());
			} else if (node.getChildren().size() > 0) {
				List<String> result = traverse(node.getChildren().get(0), transitionSchemas);
				if (result != null) ret = new ArrayList<String>(result); 
			} else {
				if (node instanceof PostgreSQLIslandScan && ((PostgreSQLIslandScan)node).getIndexCond() != null) {
					joinPredicates.add(((PostgreSQLIslandScan)node).getIndexCond().toString());
				}
				
				System.out.printf("--->> printing qualified name: %s; originalMap: %s; \n", ((SeqScan) node).getFullyQualifiedName(), originalMap);
				
				if (originalMap.get(((SeqScan) node).getFullyQualifiedName()) != null)
					ret = new ArrayList<String>(originalMap.get(((SeqScan) node).getFullyQualifiedName()));
				else {
					System.out.printf("--> tokenOfIndecision evoked at SeqScan: %s\n", ((SeqScan) node).getFullyQualifiedName());
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
			
		} else if (node instanceof Sort || node instanceof Aggregate || node instanceof Limit || node instanceof Distinct ) {
			
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
				// now the fancy largest sets problem... TODO
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
							// for each group, make a new union; reset children and make parents TODO
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
		
		
//		if (node.isQueryRoot()) {
//			remainderPermutations.add(node);
//		}
		
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
	
	public Set<String> getJoinPredicates(){
		return this.joinPredicates;
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
	

	
	@Override
	public String toString() {
		return String.format("(CIQN %s (children %s) ", name, children);
	}
	
	private void populatePredicateConnectionSets(Map<Pair<String, String>, String> jp, Map<Pair<String, String>, String> jf) {
		
//		System.out.printf("\n-----------> populate table connectivity; jp: %s; jf: %s;\n", jp, jf);
		
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
		predicateConnections = (new ConnectivityInspector<String, DefaultEdge>(sg)).connectedSets();
	}
	
	private boolean isTablesConnectedViaPredicates(Operator left, Operator right) throws Exception {
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
		
		System.out.printf("----> findIntersectionsSortByLargest result: %s\n", result);
		
		return result;
	}

	public List<CrossIslandPlanNode> getSourceVertices(CrossIslandQueryPlan ciqp) throws Exception {
		return getSourceOrTarget(ciqp, true);
	}
}

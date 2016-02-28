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
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Join.JoinType;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
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
	
	private Map<String, QueryContainerForCommonDatabase> queryContainer;
	private List<Operator> remainderPermutations;
	private List<String> remainderLoc; // if null then we need to look into the container
	
	private Set<String> children;
	private Matcher tagMatcher;
	private static DBHandler dbSchemaHandler = null;
	 
	private Map<String, ArrayList<String>> originalMap;
	
	private Set<String> joinPredicates;
	
	
	public CrossIslandQueryNode (IslandsAndCast.Scope scope, String islandQuery, String tagString) throws Exception {
		this.scope = scope;
		this.query  = islandQuery;
		this.select = (Select) CCJSqlParserUtil.parse(islandQuery);
		
		// collect the cross island children
		children = getCrossIslandChildrenReferences(tagString);
		
		if (dbSchemaHandler == null) {
			if (scope.equals(Scope.RELATIONAL))
				dbSchemaHandler = new PostgreSQLHandler(3);
			else 
				throw new Exception("Unsupported Island");
		}
		
		queryContainer = new HashMap<>();
		remainderPermutations = new ArrayList<>();
		remainderLoc = new ArrayList<>();
		joinPredicates = new HashSet<>();
		populateQueryContainer();
	}
	
	public Set<String> getCrossIslandChildrenReferences(String dawgtag) {
		
		// aka find children
		
		tagMatcher = Pattern.compile("\\b"+dawgtag+"[0-9_]+\\b").matcher(query);
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
		QueryExecutionPlan qep = new QueryExecutionPlan(scope.toString()); 
		ExecutionNodeFactory.addNodesAndEdgesNaive( qep, remainderPermutations.get(perm), remainderLoc, queryContainer, select);
		
		return qep;
	}
	
	public List<QueryExecutionPlan> getAllQEPs() throws Exception {
		
		List<QueryExecutionPlan> qepl = new ArrayList<>();
		
		// after permutation, the same SELECT this might not work
		// but we'll see
//		Select srcStmt = (Select) CCJSqlParserUtil.parse(query);
		
		for (int i = 0; i < remainderPermutations.size(); i++ ){
			QueryExecutionPlan qep = new QueryExecutionPlan(scope.toString()); 
			ExecutionNodeFactory.addNodesAndEdgesNaive( qep, remainderPermutations.get(i), remainderLoc, queryContainer, select);
			qepl.add(qep);
		}
		
		return qepl;
	}
	
	/** Setup. 
	 * 
	 * @throws Exception
	 */
	public void populateQueryContainer() throws Exception {
		
		
		// NOW WE ONLY SUPPORT RELATIONAL ISLAND
		// TODO SUPPORT OTHER ISLANDS && ISLAND CHECK 
		SQLQueryPlan queryPlan = SQLPlanParser.extractDirect((PostgreSQLHandler)dbSchemaHandler, query);
		Operator root = queryPlan.getRootNode();
		
		
		
		ArrayList<String> objs = new ArrayList<>(Arrays.asList(RelationalSignatureBuilder.sig2(query).split("\t")));
		originalMap = CatalogViewer.getDBMappingByObj(objs);
		
		
//		Select selectQuery = (Select) CCJSqlParserUtil.parse(query);
		
		
		// traverse add remainder
		remainderLoc = traverse(root, select); // this populated everything
		
		Map<String, Map<String, String>> jp = processJoinPredicates(joinPredicates);
		
//		System.out.println("\njoinPredicates: "+jp);
//		System.out.println("dataObjects: "+root.getDataObjectNames());
		
		
		if (remainderLoc == null && root.getDataObjectNames().size() > 2) {

			
			// TODO permutation happens here! it should use remainderPermutaions.get(0) as a basis and generate all other possible ones.
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
			
			
			List<Operator> permResult = getPermutatedOperators(root.getDataObjects(), jp);
			
			System.out.println("\n\n\nPermResult: ");
			int i = 1;
			for (Operator o : permResult) {
			
				System.out.printf("%d. %s\n\n", i, o.generatePlaintext(select));
				i++;
			}
			
			
			remainderPermutations.clear();
			remainderPermutations.addAll(getPermutatedOperators(root.getDataObjects(), jp));
			
			
		} // if remainderLoc is not null, then THERE IS NO NEED FOR PERMUTATIONS 
		
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
			// this case must NOT appear; throw an exception
			throw new Exception("Length of ops is 1");
			
		} else if (len == 2) {
			// the case of two
			extraction.add(makeJoin(ops.get(0), ops.get(1), null, joinPredConnections, new HashSet<>())); // TODO FIGURE OUT HOW TO INSERT THE JP
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
								
								// TODO this is where you check conditions
								// well, currently we do not prune search space
								
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
		
		if (o1.isCopy()) o1Temp = new Join(o1);
		if (o2.isCopy()) o2Temp = new Join(o2);
		
		
		for (String s : o1ns) {
			
			if (jc.get(s) == null) continue; // because jc is modified along the way
			
			Set<String> ks = jc.get(s).keySet();
			
//			System.out.printf("old o2ns: %s\n", o2ns);
			
			o2ns.retainAll(ks);
			
			
//			System.out.printf("s: %s;     keySet: %s;       \nget: %s;      o2ns: %s\n", s, ks, jc.get(s), o2ns);
			
			if (!o2ns.isEmpty()) {
				
				
				String key = o2ns.iterator().next();
				
//				System.out.println("key: "+key);
				
				// check if key has been used; TODO
				
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
	
	
	private List<String> traverse(Operator node, Select sourceQuery) throws Exception, Exception {
		// now traverse nodes and group things together
		// So the remainders should be some things that does not contain individual nodes?
		// what about mimic2v26.d_patients join d?
		// what about. If d's associated queries are from a different database, then we give mimic2v26.d_patients a name,
		//   and store the skeleton that doesn't have any reference into the remainder?
		
		// seqscan: must be in the same server
		// join: if both from the same node then yes; otherwise part of the stem
		// xx distinct, aggregate, window aggregate: BLOCKING, if they are on the same node, then give as is; otherwise it's on the stem
		// xx With: check the parent node
		// xx sort: BLOCKING
		
		// for blocking: if child is on the same node, then check parent
		//                                             if on the same node, then all on the same node
		//											   otherwise, this operator on a node, parent is on the stem
		//       if child not on the same node, then all parent branches separated

		ArrayList<String> ret = null;
		
		if (node instanceof SeqScan) {
			
			
			ret = new ArrayList<String>(originalMap.get(((SeqScan) node).getTable().getFullyQualifiedName()));
			
			
		} else if (node instanceof Join) {
			
			
			Join joinNode = (Join) node;
			Operator child0 = joinNode.getChildren().get(0);
			Operator child1 = joinNode.getChildren().get(1);
			
			List <String> c0 = traverse(child0, sourceQuery);
			List <String> c1 = traverse(child1, sourceQuery);
			
			
			if (c0 != null && c1 != null) {
				
				Set <String> intersection = new HashSet<> (c0);
				intersection.retainAll(c1);
				
				if (intersection.isEmpty()) {
					pruneChild(sourceQuery, child1, c1);
					pruneChild(sourceQuery, child0, c0);
					
				} else {
					ret = new ArrayList<String>(intersection);
				}
			}
			
			
			if (c0 == null && c1 != null) {
				pruneChild(sourceQuery, child1, c1);
			}
			
			if (c1 == null && c0 != null) {
				pruneChild(sourceQuery, child0, c0);
			} 
			
			// do nothing if both are pruned before enter here, thus saving it for the remainder 
			
			if (((Join)node).getJoinPredicateOriginal() != null)
				joinPredicates.add(((Join)node).updateOnExpression(((Join)node).getJoinPredicateOriginal(), child0, child1, new Table(), new Table(), true));
			
		} else {
			 throw new Exception("unsupported Operator in CrossIslandQueryNode");
		}
		
		
		if (node.isQueryRoot()) {
			// this status is set at SQLQueryPlan
			remainderPermutations.add(node);
		}
		
		return ret;
	}
	
	public void pruneChild(Select sourceQuery, Operator c, List<String> traverseResult) throws Exception {
		// prune c
		c.prune(true);
		
		Map<String, ConnectionInfo> cis = new HashMap<>();
		
//		System.out.println("traverse result: "+traverseResult);
		
		for (String s : traverseResult) {
			
//			System.out.println("dbid: "+s);
			
			cis.put(s, PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(s)));
		}
		
		queryContainer.put(c.getPruneToken(), new QueryContainerForCommonDatabase(cis, c, sourceQuery, c.getPruneToken()));
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
}

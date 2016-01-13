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
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import istc.bigdawg.utils.IslandsAndCast;
import istc.bigdawg.utils.IslandsAndCast.Scope;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
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
	
	
	public CrossIslandQueryNode (IslandsAndCast.Scope scope, String islandQuery, String tagString) throws Exception {
		this.scope = scope;
		this.query  = islandQuery;
		this.select = (Select) CCJSqlParserUtil.parse(islandQuery);
		
		// collect the cross island children
		children = getCrossIslandChildrenReferences(tagString);
		
		if (dbSchemaHandler == null) {
			if (scope.equals(Scope.RELATIONAL))
				dbSchemaHandler = new PostgreSQLHandler(0, 3);
			else 
				throw new Exception("Unsupported Island");
		}
		
		queryContainer = new HashMap<>();
		remainderPermutations = new ArrayList<>();
		remainderLoc = new ArrayList<>();
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
		
		
		if (remainderLoc == null) {

			
			// TODO permutation happens here! it should use remainderPermutaions.get(0) as a basis and generate all other possible ones.
			// 1. support WITH ; 
			// 2. CHANGE SORT RELATED CODE
			//
			// Permutations are done according to the structure of remainders.
			// if remainderLoc is not null, then there is only one permutation
			// otherwise, one should be able to specify an index to construct a QEP
			
			
		} // if remainderLoc is not null, then THERE IS NO NEED FOR PERMUTATIONS 
		
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
			
			
			if (c0 == null) {
				pruneChild(sourceQuery, child1, c1);
			}
			
			if (c1 == null) {
				pruneChild(sourceQuery, child0, c0);
			} 
			
			// do nothing if both are pruned before enter here, thus saving it for the remainder 
			
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
		for (String s : traverseResult) {
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

package istc.bigdawg.islands.relational.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.SQLOutItemResolver;
import istc.bigdawg.islands.relational.SQLTableExpression;
import istc.bigdawg.islands.relational.utils.SQLAttribute;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.log4j.Logger;


public class SQLIslandJoin extends SQLIslandOperator implements Join {

	private JoinType joinType = null;
	private String joinPredicate = null;
	private String joinFilter = null; 
	private List<String> aliases;
	
	protected Map<String, SQLAttribute> srcSchema;

	protected static final String BigDAWGSQLJoinPrefix = "BIGDAWGSQLJOIN_";
	protected static int maxJoinSerial = 0;
	protected Integer joinID = null;
	
	// for SQL
	public SQLIslandJoin (Map<String, String> parameters, List<String> output, SQLIslandOperator lhs, SQLIslandOperator rhs, SQLTableExpression supplement) 
			throws QueryParsingException, JSQLParserException  {
		super(parameters, output, lhs, rhs, supplement);

		// mending non-canoncial ordering
		boolean flipJoinType = false;
		if (children.get(0) instanceof SQLIslandScan && !(children.get(1) instanceof SQLIslandScan)) {
			SQLIslandOperator child0 = (SQLIslandOperator) children.get(1);
			SQLIslandOperator child1 = (SQLIslandOperator) children.get(0);
			children.clear();
			children.add(child0);
			children.add(child1);
			flipJoinType = true;
		}
		
		this.isBlocking = false;
		this.setAliases(new ArrayList<>());
		
		maxJoinSerial++;
		this.setJoinID(maxJoinSerial);
	
		srcSchema = new LinkedHashMap<>(lhs.outSchema);
		srcSchema.putAll(rhs.outSchema);
		
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);
			
			SQLOutItemResolver out = new SQLOutItemResolver(expr, srcSchema, supplement);

			SQLAttribute attr = out.getAttribute();
			
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		
		
		// process join predicates and join filters
		// if hash join "Hash-Cond", merge join "Merge-Cond"
		for(String p : parameters.keySet()) 
			if(p.endsWith("Cond")) 
				joinPredicate = parameters.get(p);
		joinFilter = parameters.get("Join-Filter");
		if (joinFilter != null)  joinFilter = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(joinFilter);
		if (joinPredicate != null) joinPredicate = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(joinPredicate);
	
		if(joinFilter != null && joinPredicate != null && joinFilter.contains(joinPredicate)) { // remove duplicate
			String joinClause = "(" + joinPredicate + ") AND"; // canonical form
			if(joinFilter.contains(joinClause)) 			
				joinFilter = joinFilter.replace(joinClause, "");
			else {
				joinClause = " AND (" + joinPredicate + ")";
				joinFilter = joinFilter.replace(joinClause, "");			
			}
		} 
		
		inferJoinParameters();
		
		for (Operator o : children) {
			if (o instanceof SQLIslandAggregate) {
				((SQLIslandAggregate)o).setSingledOutAggregate();
			}
		}

		if (parameters.containsKey("Join-Type")) {
			try {
				joinType = JoinType.valueOf(parameters.get("Join-Type"));
				// Have to potentially flip the join type as well.
				if (flipJoinType) {
					if (joinType == JoinType.Left) {
						joinType = JoinType.Right;
					} else if (joinType == JoinType.Right) {
						joinType = JoinType.Left;
					}
				}
			} catch (IllegalArgumentException e) {
				// unknown join type
				Logger.getLogger(this.getClass().getName()).warn("Unknown Join-Type returned: '" + parameters.get("Join-Type") + "'");
			}
		}
		
	}
    
	
	
	// combine join ON clause with WHEREs that combine two tables
 	// if a predicate references data that is not public, move it to the filter
 	// collect equality predicates over public attributes in joinPredicate
 	// only supports AND in predicates, not OR or NOT
 	private void inferJoinParameters() throws QueryParsingException, JSQLParserException {
 		
 		if(joinFilter == null && joinPredicate == null) return;
 		if (joinPredicate != null && joinPredicate.length() > 0) { 
 			Expression jp = CCJSqlParserUtil.parseCondExpression(joinPredicate);
 			SQLExpressionUtils.removeExcessiveParentheses(jp);
 			joinPredicate = jp.toString(); 
 		}
 		if (joinFilter != null && joinFilter.length() > 0) {
 			Expression jf = CCJSqlParserUtil.parseCondExpression(joinFilter);
 			SQLExpressionUtils.removeExcessiveParentheses(jf);
 			joinFilter = jf.toString(); 
 		}
 	}
    
 	public SQLIslandJoin (SQLIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		SQLIslandJoin j = (SQLIslandJoin) o;
		
		this.setJoinID(j.getJoinID());
		
		this.joinType = j.joinType;
		this.isCopy = j.isCopy;
		
		if (j.joinPredicate == null) this.joinPredicate = j.joinPredicate;
		else this.joinPredicate = new String(j.joinPredicate);
		
		if (j.joinFilter == null) this.joinFilter = j.joinFilter;
		else this.joinFilter = new String(j.joinFilter);

		this.srcSchema = new HashMap<>();
		try {
			for (String s : j.srcSchema.keySet()) {
				if (j.srcSchema.get(s) != null) 
					this.srcSchema.put(new String(s), new SQLAttribute(j.srcSchema.get(s)));
			}
		} catch (JSQLParserException e) {
			 throw new IslandException (e.getMessage(), e);
		}
		
		this.setAliases(new ArrayList<>());
		if (j.getAliases() != null) {
			for (String a : j.getAliases()) {
				this.getAliases().add(new String(a));
			}
		}
	}
	
	public SQLIslandJoin(Operator child0, Operator child1, JoinType jt, String joinPred, boolean isFilter) throws JSQLParserException {
		this.isCTERoot = false; // TODO VERIFY
		this.isBlocking = false;
		this.isPruned = false;
		this.isCopy = true;
		this.setAliases(new ArrayList<>());
		this.setComplexOutItemFromProgeny(new LinkedHashMap<>());
		
		maxJoinSerial++;
		this.setJoinID(maxJoinSerial);
		 
		
		if (jt != null) this.joinType = jt;
		
		if (joinPred != null) {
			if (isFilter) this.joinFilter = new String(joinPred);
			else this.joinPredicate = new String(joinPred);
		}
		
		this.isQueryRoot = true;
		
		this.dataObjects = new HashSet<>();
		
		this.srcSchema = new LinkedHashMap<String, SQLAttribute>(((SQLIslandOperator) child0).outSchema);
		srcSchema.putAll(((SQLIslandOperator)child1).outSchema);
		
		this.outSchema = new LinkedHashMap<String, SQLAttribute>(((SQLIslandOperator) child0).outSchema);
		outSchema.putAll(((SQLIslandOperator) child1).outSchema);
		
		
		this.children = new ArrayList<>();
		this.children.add(child0);
		this.children.add(child1);
		
		if (child0.isCopy()) child0.setParent(this);
		if (child1.isCopy()) child1.setParent(this);
		
		child0.setQueryRoot(false);
		child1.setQueryRoot(false);
	}
	
	public SQLIslandJoin() {
		super();

		this.isCTERoot = false; // TODO VERIFY
		this.isBlocking = false;
		this.isPruned = false;
		this.setAliases(new ArrayList<>());
		srcSchema = new LinkedHashMap<String, SQLAttribute>();
		
		maxJoinSerial++;
		this.setJoinID(maxJoinSerial);
	}

	@Override
	public Join construct(Operator child0, Operator child1, JoinType jt, String joinPred, boolean isFilter) throws Exception {

		this.isCopy = true; 
		
		if (jt != null) this.joinType = jt;
		
		if (joinPred != null) {
			if (isFilter) this.joinFilter = new String(joinPred);
			else this.joinPredicate = new String(joinPred);
		}
		
		this.isQueryRoot = true;
		
		this.dataObjects = new HashSet<>();
		
		this.srcSchema = new LinkedHashMap<>(((SQLIslandOperator) child0).outSchema);
		srcSchema.putAll(((SQLIslandOperator)child1).outSchema);
		
		this.outSchema = new LinkedHashMap<>(((SQLIslandOperator) child0).outSchema);
		outSchema.putAll(((SQLIslandOperator) child1).outSchema);
		
		this.children.add(child0);
		this.children.add(child1);
		
		if (child0.isCopy()) child0.setParent(this);
		if (child1.isCopy()) child1.setParent(this);
		
		child0.setQueryRoot(false);
		child1.setQueryRoot(false);
		
		return this;
	};
	
    @Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
    
    
    
    public String toString() {
    		return "(Join " + children.get(0).toString() + ", " + children.get(1).toString() 
    				+ ", " + joinType + ", " + joinPredicate + ", " + joinFilter + ")";
    }
    
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException {
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else {
			StringBuilder sb = new StringBuilder();
			sb.append("{join").append(children.get(0).getTreeRepresentation(false)).append(children.get(1).getTreeRepresentation(false));

			sb.append('}');
			return sb.toString();
		}
	}
	
	
	public String getJoinToken() {
		
		if (getJoinID() == null) {
			maxJoinSerial ++;
			setJoinID(maxJoinSerial);
		}
		
		return BigDAWGSQLJoinPrefix + getJoinID();
	}
	
	
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException {
		
		Operator parent = this;
		while (!parent.isBlocking() && parent.getParent() != null ) parent = parent.getParent();
		Map<String, String> aliasMapping = this.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = children.get(0).getObjectToExpressionMappingForSignature();
		Map<String, Set<String>> temp = children.get(1).getObjectToExpressionMappingForSignature();
		
		for (String s : temp.keySet()) {
			if (out.containsKey(s)) 
				out.get(s).addAll(temp.get(s));
			else 
				out.put(s, temp.get(s));
		}

		// joinFilter
		Expression e;
		try {
			if (joinFilter != null) { 
				e = CCJSqlParserUtil.parseCondExpression(joinFilter);
				if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e))
					addToOut(e, out, aliasMapping);
			}
			
			// joinPredicate
			if (joinPredicate != null) { 
				e = CCJSqlParserUtil.parseCondExpression(joinPredicate);
				if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e))
					addToOut(e, out, aliasMapping);
			}
		} catch (JSQLParserException ex) {
			throw new IslandException(ex.getMessage(), ex);
		}
		
		return out;
	}
	
	@Override
	public Map<String, Expression> getChildrenPredicates() throws Exception {
		Map<String, Expression> left = ((SQLIslandOperator) this.getChildren().get(0)).getChildrenPredicates();
		Map<String, Expression> right = ((SQLIslandOperator) this.getChildren().get(1)).getChildrenPredicates();
		
		boolean found = false; 
		for (String s : left.keySet()) {
			if (left.get(s) == null ) continue;
			List<Column> ls = SQLExpressionUtils.getAttributes(left.get(s));
			for (Column c : ls) {
				String s2 = c.getTable().getName();
				if (right.containsKey(s2)) {
					left.replace(s, null);
					found = true;
					break;
				}
			}
			if (found) break;
		}
		
		found = false; 
		for (String s : right.keySet()) {
			if (right.get(s) == null ) continue;
			List<Column> ls = SQLExpressionUtils.getAttributes(right.get(s));
			for (Column c : ls) {
				String s2 = c.getTable().getName();
				if (left.containsKey(s2)) {
					right.replace(s, null);
					found = true;
					break;
				}
			}
			if (found) break;
		}
		
		left.putAll(right);
		
		return left;
	}
	
	@Override
	public Expression resolveAggregatesInFilter(String e, boolean goParent, SQLIslandOperator lastHopOp, Set<String> names, StringBuilder sb) throws IslandException {
		
		Expression exp = null;
		if (parent != null && lastHopOp != parent && (exp = ((SQLIslandOperator) parent).resolveAggregatesInFilter(e, true, this, names, sb)) != null) 
			return exp;
		for (Operator o : children) {
			if (goParent && o == lastHopOp) continue;
			if ((exp = ((SQLIslandOperator) o).resolveAggregatesInFilter(e, false, this, names, sb)) != null) return exp;
		}
		return exp;
		
	} 
	
	@Override
	public void seekScanAndProcessAggregateInFilter() throws Exception {
		
		if (joinFilter != null) {
			joinFilter = processFilterForAggregateEntry(joinFilter);
		}
		
		if (joinPredicate != null) {
			joinPredicate = processFilterForAggregateEntry(joinPredicate);
		}
		
		super.seekScanAndProcessAggregateInFilter();
	}
	
	private String processFilterForAggregateEntry(String s) throws Exception {
		
		
		
		Expression e = CCJSqlParserUtil.parseCondExpression(s);
		
		if (!SQLExpressionUtils.isFunctionPresentInCondExpression(e)) return s;
		
		Expression left = SQLExpressionUtils.getOneSideOfBinaryCondExpression(e, true);
		while (left instanceof Parenthesis) left = ((Parenthesis) left).getExpression();
		
		StringBuilder sb = new StringBuilder();
		Set<String> names = new HashSet<>();
		Expression result = resolveAggregatesInFilter(left.toString(), true, this, names, sb);
		if (result != null) {
			SQLExpressionUtils.setOneSideOfBinaryCondExpression(result, e, true);
		}
		
		Expression right = SQLExpressionUtils.getOneSideOfBinaryCondExpression(e, false);
		while (right instanceof Parenthesis) right = ((Parenthesis) right).getExpression();
		result = resolveAggregatesInFilter(right.toString(), true, this, names, sb);
		if (result != null) {
			SQLExpressionUtils.setOneSideOfBinaryCondExpression(result, e, false);
		}
		
		return e.toString();
	}


	public Integer getJoinID() {
		return joinID;
	}


	public void setJoinID(Integer joinID) {
		this.joinID = joinID;
	}


	public List<String> getAliases() {
		return aliases;
	}


	public void setAliases(List<String> aliases) {
		this.aliases = aliases;
	}


	@Override
	public String generateJoinPredicate() throws IslandException {
		return joinPredicate != null ? new String(joinPredicate) : null;
	}

	@Override
	public String generateJoinFilter() throws IslandException {
		return joinFilter != null ? new String(joinFilter): null;
	}

	@Override
	public JoinType getJoinType() {
		return joinType;
	}
};

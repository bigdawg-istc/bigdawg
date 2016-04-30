package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.plan.extract.CommonOutItem;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.plan.generators.OperatorVisitor;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


public class Join extends Operator {

	public enum JoinType  {Left, Natural, Right};
	
	private JoinType joinType = null;
	private String joinPredicate = null;
	private String joinFilter = null; 
	private List<String> aliases;
//	private String joinPredicateOriginal = null; // TODO determine if this is useful for constructing new remainders 
//	private String joinFilterOriginal = null; 
	
	protected Map<String, DataObjectAttribute> srcSchema;
//	protected boolean joinPredicateUpdated = false;
	
	
	protected static int maxJoinSerial = 0;
	protected Integer joinID = null;
	
	
	// for SQL
	public Join (Map<String, String> parameters, List<String> output, Operator lhs, Operator rhs, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, lhs, rhs, supplement);

		// mending non-canoncial ordering
		if (children.get(0) instanceof Scan && !(children.get(1) instanceof Scan)) {
			Operator child0 = children.get(1);
			Operator child1 = children.get(0);
			children.clear();
			children.add(child0);
			children.add(child1);
		}
		
		this.isBlocking = false;
		this.setAliases(new ArrayList<>());
		
		maxJoinSerial++;
		this.setJoinID(maxJoinSerial);
	
		srcSchema = new LinkedHashMap<String, DataObjectAttribute>(lhs.outSchema);
		srcSchema.putAll(rhs.outSchema);
		
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);
			
			SQLOutItem out = new SQLOutItem(expr, srcSchema, supplement);

			DataObjectAttribute attr = out.getAttribute();
			
//			attr.setExpression(rewriteComplextOutItem(attr.getExpressionString()));
			
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
		
//		if (joinPredicate != null)
//			joinPredicateOriginal 	= new String (joinPredicate);
//		if (joinFilter != null)
//			joinFilterOriginal 		= new String (joinFilter);
		
		for (Operator o : children) {
			if (o instanceof Aggregate) {
				((Aggregate)o).setSingledOutAggregate();
			}
		}
		
		
//		System.out.printf("---> jp: %s\njf: %s\n\n", joinPredicate, joinFilter);
	}
    
	
	// for AFL
	public Join(Map<String, String> parameters, SciDBArray output, Operator lhs, Operator rhs) throws Exception  {
		super(parameters, output, lhs, rhs);

		maxJoinSerial++;
		this.setJoinID(maxJoinSerial);
		
		isBlocking = false;
		
		joinPredicate = parameters.get("Join-Predicate");
		setAliases(Arrays.asList(parameters.get("Children-Aliases").split(" ")));

		srcSchema = new LinkedHashMap<String, DataObjectAttribute>(lhs.outSchema);
		srcSchema.putAll(rhs.outSchema);
		
		// attributes
		for (String expr : output.getAttributes().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getAttributes().get(expr), false, srcSchema);
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, "Dimension", true, srcSchema);
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		inferJoinParameters();
	}
	
	// combine join ON clause with WHEREs that combine two tables
 	// if a predicate references data that is not public, move it to the filter
 	// collect equality predicates over public attributes in joinPredicate
 	// only supports AND in predicates, not OR or NOT
 	private void inferJoinParameters() throws Exception {
 		
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
    
 	public Join (Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Join j = (Join) o;
		
		this.setJoinID(j.getJoinID());
		
		this.joinType = j.joinType;
		this.isCopy = j.isCopy;
		
		if (j.joinPredicate == null) this.joinPredicate = j.joinPredicate;
		else this.joinPredicate = new String(j.joinPredicate);
		
		if (j.joinFilter == null) this.joinFilter = j.joinFilter;
		else this.joinFilter = new String(j.joinFilter);

		this.srcSchema = new HashMap<>();
		for (String s : j.srcSchema.keySet()) {
			if (j.srcSchema.get(s) != null) 
				this.srcSchema.put(new String(s), new DataObjectAttribute(j.srcSchema.get(s)));
		}
		
		this.setAliases(new ArrayList<>());
		if (j.getAliases() != null) {
			for (String a : j.getAliases()) {
				this.getAliases().add(new String(a));
			}
		}
	}
	
	public Join(Operator child0, Operator child1, JoinType jt, String joinPred, boolean isFilter) throws JSQLParserException {
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
//		this.joinReservedObjects = new HashSet<>();
		
		this.srcSchema = new LinkedHashMap<String, DataObjectAttribute>(child0.outSchema);
		srcSchema.putAll(child1.outSchema);
		
		this.outSchema = new LinkedHashMap<String, DataObjectAttribute>(child0.outSchema);
		outSchema.putAll(child1.outSchema);
		
		
		this.children = new ArrayList<>();
		this.children.add(child0);
		this.children.add(child1);
		
		if (child0.isCopy()) child0.parent = this;
		if (child1.isCopy()) child1.parent = this;
		
		child0.isQueryRoot = false;
		child1.isQueryRoot = false;
	}

    @Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
    
    private boolean replaceTableNameWithPruneName(Operator child, Expression e, Table t, List<String> itemsSet) throws Exception {
		if (child.isPruned()) {
			// does child have any of those names? 
			Set<String> names = new HashSet<>(child.getDataObjectNames());
			if (child.getObjectAliases() == null) child.updateObjectAliases();
			names.addAll(child.getObjectAliases());
			names.retainAll(itemsSet);
			if (names.size() > 0) {
				SQLExpressionUtils.renameAttributes(e, names, null, child.getPruneToken());
				t.setName(child.getPruneToken());
				return true;
			} else 
				return false;
		} else if (child instanceof Aggregate && ((Aggregate)child).getAggregateID() != null) {
			// does child have any of those names? 
			Set<String> names = new HashSet<>(child.getDataObjectAliasesOrNames().keySet());
//			names.addAll(child.objectAliases);
			
			names.retainAll(itemsSet);
			if (names.size() > 0) {
				SQLExpressionUtils.renameAttributes(e, names, null, ((Aggregate)child).getAggregateToken());
				t.setName(((Aggregate)child).getAggregateToken());
				return true;
			} else 
				return false;
		}else {
			boolean ret = false;
			for (Operator o : child.getChildren()) {
				ret = ret || replaceTableNameWithPruneName(o, e, t, itemsSet);
			}
			return ret;
		}
	}
    
    private boolean findAndGetTableName(Operator child, Table t, List<String> itemsSet) throws Exception {
    	
		Set<String> names = new HashSet<>(child.getDataObjectNames());
		child.updateObjectAliases();
		names.addAll(child.getObjectAliases());
		names.retainAll(itemsSet);
		if (names.size() > 0) {
			if (child instanceof Scan) {
				t.setName(((Scan)child).getTable().toString());
			} else if (child.getChildren().size() > 0) {
				return findAndGetTableName(child.getChildren().get(0), t, itemsSet);
			}
			return false;
		} else {
			boolean ret = false;
			for (Operator o : child.getChildren()) {
				ret = ret || findAndGetTableName(o, t, itemsSet);
			}
			return ret;
		}    	
    }
    
   
    
    
    public String toString() {
    		return "Joining " + children.get(0).toString() + " x " + children.get(1).toString() 
    				+ " type " + joinType + " predicates " + joinPredicate + " filters " + joinFilter;
    }
    
//	@Override
//	public String generateAFLString(int recursionLevel) throws Exception {
//		StringBuilder sb = new StringBuilder();
//		sb.append("cross_join(");
//		
//		if (children.get(0).isPruned())
//			sb.append(children.get(0).getPruneToken());
//		else 
//			sb.append(children.get(0).generateAFLString(recursionLevel+1));
//		
//		if (!this.getAliases().isEmpty()) 
//			sb.append(" as ").append(getAliases().get(0));
//		sb.append(", ");
//		
//		if (children.get(1).isPruned())
//			sb.append(children.get(1).getPruneToken());
//		else 
//			sb.append(children.get(1).generateAFLString(recursionLevel+1));
//		
//		if (!this.getAliases().isEmpty()) sb.append(" as ").append(getAliases().get(1));
//		
//		if (joinPredicate != null) {
//			sb.append(", ");
//			sb.append(joinPredicate.replaceAll("( AND )|( = )", ", ").replaceAll("[<>= ()]+", " ").replace("\\s+", ", "));
//		}
//		
//		sb.append(')');
//		return sb.toString();
//	}
	
	public String getOriginalJoinPredicate() {
		return joinPredicate != null ? new String(joinPredicate) : null;
	}
	public String getOriginalJoinFilter() {
		return joinFilter != null ? new String(joinFilter): null;
	}
	
//	private void addJSQLParserJoin(Select dstStatement, Table t) {
//		net.sf.jsqlparser.statement.select.Join newJ = new net.sf.jsqlparser.statement.select.Join();
//    	newJ.setRightItem(t);
//    	newJ.setSimple(true);
//    	if (((PlainSelect) dstStatement.getSelectBody()).getJoins() == null)
//    		((PlainSelect) dstStatement.getSelectBody()).setJoins(new ArrayList<>());
//    	((PlainSelect) dstStatement.getSelectBody()).getJoins().add(newJ);
//	}
//	
//	public String getJoinPredicate(){
//		return joinPredicate;
//	};
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else {
			StringBuilder sb = new StringBuilder();
			sb.append("{join").append(children.get(0).getTreeRepresentation(false)).append(children.get(1).getTreeRepresentation(false));

//			if (joinFilter != null && !joinFilter.isEmpty()) {
//				Expression e = CCJSqlParserUtil.parseCondExpression(joinFilter);
//				SQLExpressionUtils.removeExcessiveParentheses(e);
//				sb.append(SQLExpressionUtils.parseCondForTree(e));
//			}
//			
//			List<Operator> treeWalker = this.getChildren();
//			while (!treeWalker.isEmpty()) {
//				List<Operator> nextgen = new ArrayList<>();
//				for (Operator o : treeWalker) {
//					if (o instanceof Join) continue;
//					if (o instanceof Scan && ((Scan) o).indexCond != null) {
//						sb.append(SQLExpressionUtils.parseCondForTree(((Scan)o).indexCond));
//					} else {
//						nextgen.addAll(o.getChildren());
//					}
//				}
//				treeWalker = nextgen;
//			}
			sb.append('}');
			return sb.toString();
		}
	}
	
	
	public String getJoinToken() {
		
		if (getJoinID() == null) {
			maxJoinSerial ++;
			setJoinID(maxJoinSerial);
		}
		
		return "BIGDAWGJOINTOKEN_"+getJoinID();
	}
	
	
	
//	/**
//	 * This one only supports equal sign and Column expressions
//	 * @return
//	 * @throws Exception
//	 */
//	@Deprecated
//	public List<String> getJoinPredicateObjectsForBinaryExecutionNode() throws Exception {
//		
//		List<String> ret = new ArrayList<String>();
//		
//		if (joinPredicate == null || joinPredicate.length() == 0) {
//			
//			Expression extraction = null;
//			Column leftColumn = null;
//			Column rightColumn = null;
//			
//			
//			List<String> ses = processLeftAndRightWithIndexCond(true, null);
//			String s, s2; 
//			if (ses != null) {
//				s = ses.get(0);
//				s2 = ses.get(1);
//				extraction = this.getChildren().get(0).getChildrenIndexConds().get(s);
//			} else {
//				ses = processLeftAndRightWithIndexCond(false, null);
//				if (ses == null) return ret;
//				s = ses.get(1);
//				s2 = ses.get(0);
//				extraction = this.getChildren().get(1).getChildrenIndexConds().get(s2);
//			}
//			
//			List<Column> ls = SQLExpressionUtils.getAttributes(extraction);
//			for (Column c2 : ls) if (c2.getTable().getName().equals(s)) {leftColumn = c2; break;}
//			for (Column c2 : ls) if (c2.getTable().getName().equals(s2)) {rightColumn = c2; break;}
//			
//			while (extraction instanceof Parenthesis) extraction = ((Parenthesis)extraction).getExpression();
//			ret.add(SQLExpressionUtils.getBinaryExpressionOperatorToken(extraction));
//			ret.add(String.format("{%s, %s}", leftColumn.getTable().getFullyQualifiedName(),leftColumn.getColumnName()));
//			ret.add(String.format("{%s, %s}", rightColumn.getTable().getFullyQualifiedName(),rightColumn.getColumnName()));
//			
//        	return ret;
//		}
//			
//		
//		
//		Set<String> leftChildObjects = this.getChildren().get(0).getDataObjectNames();
//
////		System.out.println("---> Left Child objects: "+leftChildObjects.toString());
////		System.out.println("---> Right Child objects: "+rightChildObjects.toString());
////		System.out.println("---> joinPredicate: "+joinPredicate);
//		
//		Expression e = CCJSqlParserUtil.parseCondExpression(joinPredicate);
//		
//		
//		while (e instanceof Parenthesis)
//			e = ((Parenthesis)e).getExpression();
//		
//		
//		ret.add(SQLExpressionUtils.getBinaryExpressionOperatorToken(e));
//		
//		
//		// TODO SUPPORT MORE THAN COLUMN?
//		
//		Column left = (Column)((EqualsTo)e).getLeftExpression();
//		Column right = (Column)((EqualsTo)e).getRightExpression();
//		
//		if (leftChildObjects.contains(left.getTable().getName()) || leftChildObjects.contains(left.getTable().getFullyQualifiedName())) {
//			ret.add(String.format("{%s, %s}", left.getTable().getFullyQualifiedName(),left.getColumnName()));
//			ret.add(String.format("{%s, %s}", right.getTable().getFullyQualifiedName(),right.getColumnName()));
//		} else {
//			ret.add(String.format("{%s, %s}", right.getTable().getFullyQualifiedName(),right.getColumnName()));
//			ret.add(String.format("{%s, %s}", left.getTable().getFullyQualifiedName(),left.getColumnName()));
//		}
//		
////		System.out.println("---> joinPredicate ret: "+ret.toString()+"\n\n\n");
//		
//		return ret;
//	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		
		Operator parent = this;
		while (!parent.isBlocking && parent.parent != null ) parent = parent.parent;
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
		
		return out;
	}
	
	@Override
	public Map<String, Expression> getChildrenPredicates() throws Exception {
		Map<String, Expression> left = this.getChildren().get(0).getChildrenPredicates();
		Map<String, Expression> right = this.getChildren().get(1).getChildrenPredicates();
		
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
		
//		if (this.joinID != null && joinPredicate != null) {
//			Expression e = CCJSqlParserUtil.parseCondExpression(joinPredicate);
//			System.out.printf("\n--><><><> expression: %s; \n", e);
//			String out = SQLExpressionUtils.getRelevantFilterSections(e, getChildren().get(0).getDataObjectAliasesOrNames().keySet(), getChildren().get(1).getDataObjectAliasesOrNames().keySet());
//			System.out.printf("--><><><> out: %s; \n", out);
//			if (out.length() > 0)
//				left.put(this.getJoinToken(), CCJSqlParserUtil.parseCondExpression(out));
//		}
//		if (this.joinID != null && joinFilter != null) {
//			left.put(this.getJoinToken(), CCJSqlParserUtil.parseCondExpression(SQLExpressionUtils.getRelevantFilterSections(CCJSqlParserUtil.parseCondExpression(joinFilter), left.keySet(), right.keySet())));
//		}
		return left;
	}
	
	@Override
	public Expression resolveAggregatesInFilter(String e, boolean goParent, Operator lastHopOp, Set<String> names, StringBuilder sb) throws Exception {
		
		Expression exp = null;
		if (parent != null && lastHopOp != parent && (exp = parent.resolveAggregatesInFilter(e, true, this, names, sb)) != null) 
			return exp;
		for (Operator o : children) {
			if (goParent && o == lastHopOp) continue;
			if ((exp = o.resolveAggregatesInFilter(e, false, this, names, sb)) != null) return exp;
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


//	public String getJoinFilter() {
//		return joinFilter;
//	}


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
};

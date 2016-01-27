package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import istc.bigdawg.extract.logical.SQLExpressionHandler;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.utils.sqlutil.SQLUtilities;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;


public class Join extends Operator {

	public enum JoinType  {Left, Natural, Right};
	
	private String currentWhere = "";
	private JoinType joinType = null;
	private String joinPredicate = null;
	private String joinFilter; 
	private String joinPredicateOriginal = null; // TODO determine if this is useful for constructing new remainders 
	private String joinFilterOriginal = null; 
	

	protected Map<String, SQLAttribute> srcSchema;
	protected boolean joinPredicateUpdated = false;
	
	public Join (Operator o) throws Exception {
		super(o);
		Join j = (Join) o;
		this.currentWhere = new String(j.currentWhere);
		this.joinType = j.joinType;
		this.joinPredicate = new String(j.joinPredicate);
		for (String s : j.srcSchema.keySet()) {
			this.srcSchema.put(new String(s), new SQLAttribute(j.srcSchema.get(s)));
		}
	}
	
    Join(Map<String, String> parameters, List<String> output, Operator lhs, Operator rhs, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, lhs, rhs, supplement);

		isBlocking = false;
		
	
		// if hash join "Hash-Cond", merge join "Merge-Cond"
		for(String p : parameters.keySet()) {
			if(p.endsWith("Cond")) {
				joinPredicate = parameters.get(p);
			}
		}
		

		// if hash join
		joinPredicate = SQLUtilities.parseString(joinPredicate);
		joinFilter = parameters.get("Join-Filter");
		joinFilter = SQLUtilities.parseString(joinFilter);


		
	
		if(joinFilter != null && joinFilter.contains(joinPredicate)) { // remove duplicate
			String joinClause = "(" + joinPredicate + ") AND"; // canonical form
			System.out.println("Deleting extra " + joinPredicate);
			if(joinFilter.contains(joinClause)) {				
				joinFilter = joinFilter.replace(joinClause, "");
			}
			else {
				joinClause = " AND (" + joinPredicate + ")";
				
				joinFilter = joinFilter.replace(joinClause, "");			
			}
		}
		
		currentWhere = joinFilter;
		
		srcSchema = new LinkedHashMap<String, SQLAttribute>(lhs.outSchema);
		srcSchema.putAll(rhs.outSchema);
		
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);
			
			SQLOutItem out = new SQLOutItem(expr, srcSchema, supplement);

			SQLAttribute attr = out.getAttribute();
			String attrName = attr.getName();		
			outSchema.put(attrName, attr);
				
		}
		inferJoinParameters();
		
		if (joinPredicate != null)
			joinPredicateOriginal 	= new String (joinPredicate);
		if (joinFilter != null)
			joinFilterOriginal 		= new String (joinFilter);
	}
    
 // combine join ON clause with WHEREs that combine two tables
 	// if a predicate references data that is not public, move it to the filter
 	// collect equality predicates over public attributes in joinPredicate
 	// only supports AND in predicates, not OR or NOT
 	private void inferJoinParameters() throws Exception {
 		
 		if(joinFilter == null && joinPredicate == null) {
 			return;
 		}
 		
 		if(joinFilter != null && (joinFilter.matches(" OR ") || joinFilter.matches(" NOT "))) {
 			throw new Exception("OR and NOT predicates not yet implemented for joins.");
 		}
 		
 		if(joinPredicate != null && (joinPredicate.matches(" OR ") || joinPredicate.matches(" NOT "))) {
 			throw new Exception("OR and NOT predicates not yet implemented for joins.");
 		}

 		
 		final List<EqualsTo> allEqualities = new ArrayList<EqualsTo>();
 		final List<BinaryExpression> allComparisons = new ArrayList<BinaryExpression>();
 		
 		String allJoins = new String();
 		
 		if(joinPredicate != null) {
 			allJoins += joinPredicate;
 			if(joinFilter != null && joinFilter.length() > 0) {
 				allJoins += " AND ";
 			}
 		}
 		
 		allJoins += joinFilter;
 		
 	    
 	    
 		SQLExpressionHandler deparser = new SQLExpressionHandler() {
 	        
 		    @Override
 		    public void visit(EqualsTo equalsTo) {
 		    	super.visit(equalsTo);
 		    	allEqualities.add(equalsTo);
 		    }
 			
 		    @Override
 		    public void visit(LikeExpression likeExpression) {
 		    	super.visit(likeExpression);
 		    	allComparisons.add(likeExpression);
 		    }
 		    
 		    @Override
 		    public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
 		    	super.visitOldOracleJoinBinaryExpression(expression, operator);
 		    	allComparisons.add(expression);

 		    }
 		    
 	    };

 	    StringBuilder b = new StringBuilder();
 	    deparser.setBuffer(b);
 	    Expression expr = CCJSqlParserUtil.parseCondExpression(allJoins);
 	    expr.accept(deparser);

 	    
 	    
 	    Map<String, Integer> joinOn = new LinkedHashMap<String, Integer>();
 	    Map<String, Integer> filters = new LinkedHashMap<String, Integer>();
 	    
 	    List<EqualsTo> publicEqualities = new ArrayList<EqualsTo>();
 	    for (EqualsTo eq : allEqualities) {
    		joinOn.put(eq.toString(), 1);
    		publicEqualities.add(eq);	  
 	    }
 	    
 	    Set<String> joinOnDistinct = joinOn.keySet();
 	    
 	    for(int i = 0; i < allComparisons.size(); ++i) {
 	    	if(!publicEqualities.contains(allComparisons.get(i))) {
 	    		filters.put(allComparisons.get(i).toString(), 1);
 	    	}
 	    }
 	

 		joinFilter = StringUtils.join(filters.keySet().toArray(), " AND ");
 		joinPredicate = StringUtils.join(joinOnDistinct.toArray(), " AND ");
 		
 	}
    
    
	
    
//    @Override
//	public List<SQLAttribute> getSliceKey() throws JSQLParserException {
//		List<SQLAttribute> sliceKey = new ArrayList<SQLAttribute>();
//
//		Expression joinOn = parsedJoin.getOnExpression();
//		if(joinOn != null) {
//			List<String> candidateKeys = SQLExpressionUtils.getAttributes(joinOn);
//		
//			for(String k : candidateKeys) {
//				SQLAttribute a = srcSchema.get(k);
//				sliceKey.add(a);
//			}
//		}
//		return sliceKey;
//		
//	}

    @Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {
		
    	Set<String> filterSet = new HashSet<String>();
    	PlainSelect ps = null;
    	
		if (isPruned) {
			Table t = new Table();
			t.setName(this.getPruneToken());
			dstStatement = SelectUtils.buildSelectFromTable(t);
			
			return dstStatement;
		}
    	
    	Operator child0 = children.get(0);
    	Operator child1 = children.get(1);
    	
    	
    	
    	// constructing the ON expression

    	// TODO ASSUMPTION: no more than one predicate
    	
    	
    	Table t0 = new Table();
		Table t1 = new Table();
    	
    	if (joinPredicate != null) {
    		
    		joinPredicate = updateOnExpression(joinPredicate, child0, child1, t0, t1, true);
    		joinPredicateUpdated = true;
    		// ^ ON predicate constructed
    		
    	} else {
        	Set<String> child0ObjectSet = child0.getDataObjects();
        	Set<String> child1ObjectSet = child1.getDataObjects();

//    		// for debugging
//        	System.out.printf("\nJP: %s\nchild0obj: %s\nchild1obj: %s\n\n", joinPredicate, 
//        			child0ObjectSet, child1ObjectSet);
    		
        	t0.setName((String)child0ObjectSet.toArray()[0]);
        	t1.setName((String)child1ObjectSet.toArray()[0]);
    	}
    	
    	
		
		
		
		Expression expr = null;
    	if(joinPredicate != null && !joinPredicate.isEmpty()) {
    		expr = CCJSqlParserUtil.parseCondExpression(joinPredicate);
    	} else {
    		expr = null;
    	}
		
    	
    	
    	
    	
    	// check parent reservation and mark this node's usage
    	// TODO update all operators to use getJoinReservedObjectsFromParents();
    	
    	boolean dstStatementStartedNull = false;
    	boolean t0toAddJoin = false;
    	
    	if (dstStatement == null) {
			dstStatement = SelectUtils.buildSelectFromTable(t0);
			dstStatementStartedNull = true;
			
			this.joinReservedObjects.add(t0.getFullyQualifiedName());
			this.joinReservedObjects.add(t1.getFullyQualifiedName());
		} else {
			
			getJoinReservedObjectsFromParents();
			
			if (this.joinReservedObjects.contains(t0.getFullyQualifiedName())) {
				this.joinReservedObjects.add(t1.getFullyQualifiedName());
			} else {
				t0toAddJoin = true;
				this.joinReservedObjects.add(t0.getFullyQualifiedName());
			}
		}
    		
    	
    	
		// modify the dstStatement
    	
    	// start at child 0
		dstStatement = child0.generatePlaintext(srcStatement, dstStatement); 
		ps = (PlainSelect) dstStatement.getSelectBody();
		if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
	
		// proceed to the bridge, this node
		if (dstStatementStartedNull) {
			if (joinPredicate == null) 
				addJSQLParserJoin(dstStatement, t1);
			else SelectUtils.addJoin(dstStatement, t1, expr);
		} else {
			if (t0toAddJoin) {
				if (joinPredicate == null) addJSQLParserJoin(dstStatement, t0);
				else SelectUtils.addJoin(dstStatement, t0, expr);
			} else {
				if (joinPredicate == null) addJSQLParserJoin(dstStatement, t1);
				else SelectUtils.addJoin(dstStatement, t1, expr);
			}
		}
		
		// finish with child 1
		dstStatement = child1.generatePlaintext(srcStatement, dstStatement); 
		ps = (PlainSelect) dstStatement.getSelectBody();
		if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
		
    	
    	
		
		
		if(joinFilter != null) {
			
			filterSet.add(joinFilter);

//			// not sure if useful
//			if(ps.getWhere() != null) {
//				filterSet.add(ps.getWhere().toString());
//			}
			
			currentWhere = StringUtils.join(filterSet, " AND ");
			
			if (!currentWhere.isEmpty()) {
				Expression where = 	CCJSqlParserUtil.parseCondExpression(currentWhere);
				ps.setWhere(where);
			}

		}
		return dstStatement;

	}
    
    
    public String updateOnExpression(String joinPred, Operator child0, Operator child1, Table t0, Table t1, boolean update) throws Exception {
    	
    	if ((!update) && joinPredicateUpdated)
    		return joinPredicate;
    	
		BinaryExpression on = (BinaryExpression) CCJSqlParserUtil.parseCondExpression(joinPred);
		
		Expression l = on.getLeftExpression();
		Expression r = on.getRightExpression();

		
		// TODO ASSUMPTION: THERE CAN ONLY BE COLUMNS
		if (l.getClass().equals(Parenthesis.class)) {
			l = ((Parenthesis) l).getExpression();
			on.setLeftExpression(l);
		}
		if (r.getClass().equals(Parenthesis.class)) {
			r = ((Parenthesis) r).getExpression();
			on.setRightExpression(r);
		}
			
		
		Column cLeft  = ((Column) l);
		Column cRight = ((Column) r);
		
		
		String cLeftColName = cLeft.getTable().getName()+"."+cLeft.getColumnName();
		String cRightColName = cRight.getTable().getName()+"."+cRight.getColumnName();
		
		
		
		t0.setName(cLeft.getTable().getName());
		t1.setName(cRight.getTable().getName());
		
		setTableNameFromPrunedOne(child0, t0, cLeftColName, false);
		setTableNameFromPrunedOne(child1, t1, cRightColName, false);
    		
		cLeft.setTable(t0);
		cRight.setTable(t1);
		
		return on.toString();
	}
    
    
    
    private boolean setTableNameFromPrunedOne(Operator o, Table t, String fullyQualifiedName, boolean found) throws Exception{
		if (o.getOutSchema().containsKey(fullyQualifiedName)) {
			if (o.isPruned()) {
				t.setName(o.getPruneToken());
//				System.out.printf("FOUND: FQN: %s,   outSchema: %s\n", fullyQualifiedName, o.getOutSchema());
				return true;
			} else {
				if (o.getClass().equals(Join.class)) {
	    			if (setTableNameFromPrunedOne(o.getChildren().get(0), t, fullyQualifiedName, found)) 
	    				return true;
	    			else 
	    				return setTableNameFromPrunedOne(o.getChildren().get(1), t, fullyQualifiedName, found);
	    		} else {
	    			if (o.getChildren().size() == 0) return false;
	    			return setTableNameFromPrunedOne(o.getChildren().get(0), t, fullyQualifiedName, found);
	    		}
			}
		} else {
//			System.out.printf("NOT FOUND: FQN: %s,   outSchema: %s\n", fullyQualifiedName, o.getOutSchema());
		}
		
		return false;
    }
	
    public String toString() {
    		return "Joining " + children.get(0).toString() + " x " + children.get(1).toString() 
    				+ " type " + joinType + " predicates " + joinPredicate + " filters " + currentWhere;
    }
    
	@Override
	public String printPlan(int recursionLevel) {
		String planStr =  "Join(";
		planStr += children.get(0).printPlan(recursionLevel+1);
		planStr += ",";
		planStr +=  children.get(1).printPlan(recursionLevel+1);
		planStr += ", " + joinPredicate + ", " + joinFilter + ")";
		return planStr;
	}
	
	public String getJoinPredicateOriginal() {
		return joinPredicateOriginal;
	}
	public String getJoinFilterOriginal() {
		return joinFilterOriginal;
	}
	
	private void addJSQLParserJoin(Select dstStatement, Table t) {
		net.sf.jsqlparser.statement.select.Join newJ = new net.sf.jsqlparser.statement.select.Join();
    	newJ.setRightItem(t);
    	newJ.setSimple(true);
    	if (((PlainSelect) dstStatement.getSelectBody()).getJoins() == null)
    		((PlainSelect) dstStatement.getSelectBody()).setJoins(new ArrayList<>());
    	((PlainSelect) dstStatement.getSelectBody()).getJoins().add(newJ);
	}
	
	
};

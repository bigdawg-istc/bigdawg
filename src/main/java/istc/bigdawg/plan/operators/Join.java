package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.extract.logical.SQLExpressionHandler;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import istc.bigdawg.utils.sqlutil.SQLUtilities;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;

import net.sf.jsqlparser.schema.Column;

// Join can run in 3 modes: public, split, private
// if it is in split mode then Alice and Bob perform partial join locally 
// and do distributed comparisons as needed
// split join is possible when there are no blocking ops between leaf and this

public class Join extends Operator {

	public enum JoinType  {Left, Natural, Right};
	
	private String currentWhere = "";
	private boolean lhsHasJoinPred = false;
	private JoinType joinType = null;
	private String joinPredicate = null;
	private String joinFilter; 
//	private net.sf.jsqlparser.statement.select.Join parsedJoin;
	

	protected Map<String, SQLAttribute> srcSchema;
	
	public Join (Operator o) throws Exception {
		super(o);
		Join j = (Join) o;
		this.currentWhere = new String(j.currentWhere);
		this.lhsHasJoinPred = j.lhsHasJoinPred;
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


		
		if (lhs instanceof Scan) {
			lhsHasJoinPred = true;
		}
		
		
	
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
    	
    	if (lhsHasJoinPred) {
    		child1 = children.get(0);
        	child0 = children.get(1);
    	}
    	
    	if (child0.isPruned() || child1.isPruned()) {
    		Table t0 = new Table();
    		Table t1 = new Table();
			
    		t0.setName(child0.getPruneToken());
    		t1.setName(child1.getPruneToken());
    		
			dstStatement = SelectUtils.buildSelectFromTable(t0);
			
			BinaryExpression on = (BinaryExpression) CCJSqlParserUtil.parseCondExpression(joinPredicate);
			
//			parsedJoin.setRightItem(t1);
//			BinaryExpression on = (BinaryExpression) parsedJoin.getOnExpression();
			
			Column cLeft;
			Column cRight;
			
			Expression l = on.getLeftExpression();
			Expression r = on.getRightExpression();

			if (l.getClass().equals(net.sf.jsqlparser.expression.Parenthesis.class))
				cLeft = (Column) ((net.sf.jsqlparser.expression.Parenthesis) l).getExpression();
			else
				cLeft  = ((Column) on.getLeftExpression());
			
			if (r.getClass().equals(net.sf.jsqlparser.expression.Parenthesis.class)) 
				cRight = (Column) ((net.sf.jsqlparser.expression.Parenthesis) r).getExpression();
			else 
				cRight = ((Column) on.getRightExpression());
			
			
			
			String cLeftColName = cLeft.getTable().getName()+"."+cLeft.getColumnName();
			
			if (child0.getOutSchema().keySet().contains(cLeftColName)) {
				cLeft.setTable(t0);
				cRight.setTable(t1);
			} else {
				cLeft.setTable(t1);
				cRight.setTable(t0);
			}
			
			joinPredicate = on.toString();
    	}
    	
    	
		dstStatement = child0.generatePlaintext(srcStatement, dstStatement); // this should have already taken care of the pruning
		ps = (PlainSelect) dstStatement.getSelectBody();
		if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
	
		dstStatement = child1.generatePlaintext(srcStatement, dstStatement); // this should have already taken care of the pruning
		ps = (PlainSelect) dstStatement.getSelectBody();
		if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
		
		
		// child1 of join should always be a scan
    	assert(child1 instanceof Scan);
    	Scan newScan = (Scan) child1;
    	
    	Expression expr = null;
    	if(joinPredicate != null && !joinPredicate.isEmpty()) {
    		expr = CCJSqlParserUtil.parseCondExpression(joinPredicate);
    	}
    	
    	
    	if (child0.isPruned() || child1.isPruned()) {
    		Table t1 = new Table();
    		t1.setName(child1.getPruneToken());
    		SelectUtils.addJoin(dstStatement, t1, expr);
    	} else {
    		SelectUtils.addJoin(dstStatement, newScan.getTable(), expr);    		
    	}
		
		if(joinFilter != null) {
			
			filterSet.add(joinFilter);

//			// not sure if these are useful
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
	
};
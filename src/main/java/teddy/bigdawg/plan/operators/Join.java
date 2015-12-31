package teddy.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import teddy.bigdawg.schema.SQLAttribute;
import teddy.bigdawg.extract.logical.SQLTableExpression;
import teddy.bigdawg.plan.SQLQueryPlan;
import teddy.bigdawg.plan.extract.SQLOutItem;
import teddy.bigdawg.util.SQLExpressionUtils;
import teddy.bigdawg.util.SQLUtilities;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

// Join can run in 3 modes: public, split, private
// if it is in split mode then Alice and Bob perform partial join locally 
// and do distributed comparisons as needed
// split join is possible when there are no blocking ops between leaf and this

public class Join extends Operator {

	public enum JoinType  {Left, Natural, Right};
	
	
	
	private JoinType joinType = null;
	private String joinPredicate = null;
	private String joinFilter; 
	private String currentWhere;
	private net.sf.jsqlparser.statement.select.Join parsedJoin;

	protected Map<String, SQLAttribute> srcSchema;
	
	
    Join(Map<String, String> parameters, List<String> output, Operator lhs, Operator rhs, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, lhs, rhs, supplement);

		isBlocking = false;
		
		// if lhs or rhs is replicated, then isLocal = true
//		if(isReplicated(lhs) || isReplicated(rhs)) {
//			isLocal = true;
//		}
//		else {
//			isLocal = false;
//		}
		
	
		// if hash join
		joinPredicate = parameters.get("Hash-Cond");
		joinPredicate = SQLUtilities.parseString(joinPredicate);
		joinFilter = parameters.get("Join-Filter");
		joinFilter = SQLUtilities.parseString(joinFilter);

//		System.out.print("\n@@---> RHS "+rhs.getClass().toString() +" ++++++ ");
//		if (rhs instanceof Scan)
//			System.out.println(((Scan)rhs).getTable().getFullyQualifiedName());
//		else if (rhs instanceof CommonSQLTableExpressionScan)
//			System.out.println(((CommonSQLTableExpressionScan)rhs).getTable().getFullyQualifiedName());
//		else if (rhs instanceof Join) {
//			System.out.println(((Join)rhs).getChildren().get(0).toString() + " xxxx " + ((Join)rhs).getChildren().get(1).toString());
//		}
		
		if(rhs instanceof Scan) {
			Table t = ((Scan) rhs).getTable();
			parsedJoin = supplement.getJoin(t);
		} 
		
		if (parsedJoin == null && lhs instanceof Scan) {
			Table t = ((Scan) lhs).getTable();
			parsedJoin = supplement.getJoin(t);
		}
		
		if(parsedJoin == null){
			throw new Exception("Ambiguous source data for join!");
		}
		
		
		if(parsedJoin != null) {
			Expression joinOn = parsedJoin.getOnExpression();
			if(joinOn != null) {
				joinPredicate = joinOn.toString();
			}
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

//		inferSecureCoordination();
		
	}
	
//    private void inferSecureCoordination() throws JSQLParserException {
//    	Expression joinOn = parsedJoin.getOnExpression(); 
//    	List<String> attrs = new ArrayList<String>();
//    	if(joinOn != null) {
//    		 attrs = SQLExpressionUtils.getAttributes(joinOn);
//    	}
//    	
//    	if(joinFilter != null) {
//    		Expression filterExpr = CCJSqlParserUtil.parseCondExpression(joinFilter);
//    		attrs.addAll(SQLExpressionUtils.getAttributes(filterExpr));
//    	}
//    	
//    	for(String a : attrs) {
//    		SQLAttribute sa = srcSchema.get(a);
//    		updateSecurityPolicy(sa);
//    		
//     	}
//    	
//    	
//    }
    
    
//    private boolean isReplicated(Operator o) {
//    	for(SQLAttribute s : o.outSchema.values()) {
//    		if(s.getReplicated() == false) {
//    			return false;
//    		}
//    	}
//    	
//    	return true;
//    	
//    }
    
    @Override
	public List<SQLAttribute> getSliceKey() throws JSQLParserException {
		List<SQLAttribute> sliceKey = new ArrayList<SQLAttribute>();

		Expression joinOn = parsedJoin.getOnExpression();
		if(joinOn != null) {
			List<String> candidateKeys = SQLExpressionUtils.getAttributes(joinOn);
		
			for(String k : candidateKeys) {
				SQLAttribute a = srcSchema.get(k);
//				if(a.getSecurityPolicy().equals(SQLAttribute.SecurityPolicy.Public)) {
					sliceKey.add(a);
//				}
			}
		}
		return sliceKey;
		
	}

    @Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {
		
    	Set<String> filterSet = new HashSet<String>();
    	PlainSelect ps = null;
    	
		for(int i = 0; i < children.size(); ++i) {
			dstStatement = children.get(i).generatePlaintext(srcStatement, dstStatement); // ?
			
			ps = (PlainSelect) dstStatement.getSelectBody();
			if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
		}
		
//		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		List<net.sf.jsqlparser.statement.select.Join> joins = ps.getJoins();

		if(joins == null) {
			joins = new ArrayList<net.sf.jsqlparser.statement.select.Join>();
		}
		
		joins.add(parsedJoin);
		ps.setJoins(joins);
		
		
		if(joinFilter != null) {
			
			filterSet.add(joinFilter);
			
			String temp = "";
			for (String s : filterSet) {
				if (temp.equals("")) {
					temp = s;
				} else {
					temp = s + " AND " + temp;
				}
			}
			
			Expression where = 	CCJSqlParserUtil.parseCondExpression(temp);

			ps.setWhere(where);
			currentWhere = temp;

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
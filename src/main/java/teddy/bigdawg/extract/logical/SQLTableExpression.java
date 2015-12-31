package teddy.bigdawg.extract.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import teddy.bigdawg.plan.operators.Sort;
import teddy.bigdawg.plan.operators.Sort.SortOrder;

import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

//built once per SELECT block (main select or CTE)
public class SQLTableExpression {
	
	
    private List<AnalyticExpression>  analyticExpressions;
    private List<OrderByElement> orderBy = null;  // order by is all-or-nothing

    // look up alternate name for column or expression that is aliased.
    private Map<String, String>  attributeAliases;
    private int analyticExpressionIdx;
    private int sortIdx; // iterate over sorts - they may come from OVER or ORDER BY
    private List<ArrayList<OrderByElement>> sortList = null;
    private PlainSelect parsedStatement; // usually contains a WithItem, PlainSelect, or both
    private List<Join> joins;
    
    String name = null;
	
    public SQLTableExpression()  {
    	 analyticExpressions = new ArrayList<AnalyticExpression>();
    	 attributeAliases = new HashMap<String, String>();
    	 joins = new ArrayList<Join>();
    	 
    	 analyticExpressionIdx = 0;  // iterate over this when we encounter analytics in the plan
    	 sortIdx = 0;
    	 
    }
    
    public void addAnalyticExpression(AnalyticExpression a) {
    	analyticExpressions.add(a);
    	
    	// update it for the new values
    	if(sortList != null && a.getOrderByElements().size() > 0) {
    		createSortList();
    	}
    }

    public void setAnalyticExpressions(List<AnalyticExpression> aexprs) {
    	analyticExpressions = aexprs;
    }
    
    public void setOrderBy(List<OrderByElement> ord) {
    	orderBy = ord;
    	if(sortList != null) {
    		createSortList();
    	}

    	
    }
    
    public void setSelect(PlainSelect s) {
    	parsedStatement = s;
    }

    public void addJoin(Join j) {
    	joins.add(j);
    }
    
    
    public String printJoins() {
    	return joins.toString();
    }
    
    // takes in a table from the query tree
    // if it is associated with a join, return join
    public Join getJoin(Table rhs) {
//    	System.out.println("Searching for table " + rhs);
    	for(Join j : joins) {
    		FromItem from = j.getRightItem();
//    		System.out.println("Checking out " + j + " from " + from);
 
	   		if(from instanceof Table) {
	    			Table t = (Table) from;
//	        		System.out.println("Names: " + t.getName() + "," + rhs.getName());
	    			
	        		if(rhs.getAlias() != null) {
	        			if(rhs.getName().equals(t.getName()) && rhs.getAlias().getName().equals(t.getAlias().getName())) {
	        				return j;
	        			}
	        			
	        		}
	        		else { // no alias, just compare table names
	        			if(rhs.getName().equals(t.getName())) {
	        				return j;
	        			}
	        		}
    		}
    	}
    	
    	return null;
    	
     }
    
    public void addAlias(SelectExpressionItem s) {
    	assert(s.getAlias() != null);
    	String dst = s.getAlias().getName();
    	String src = s.getExpression().toString();
    	Expression e = s.getExpression();
    	
    	if(e instanceof AnalyticExpression) {
    		// capture expression handling of sql plan generator
    		src = ((AnalyticExpression) e).getName() + "() OVER (?)";
    		
    	}
    	
    	attributeAliases.put(src, dst);
    	
    }
    
    public boolean hasDistinct() {
    	return parsedStatement.getDistinct() != null;
    }
    
    
    public List<Expression> getGroupBy() {
    	return parsedStatement.getGroupByColumnReferences();
    }
    
    public void setAliases(Map<String, String> attrAliases) {
    	attributeAliases = attrAliases;
    }
    
    
    public List<AnalyticExpression> getAnalyticExpressions() {
    	return analyticExpressions;
    }
    
    public List<OrderByElement>  getOrderByClause() {
    	return orderBy;
    }
    
    public Map<String, String> getAliases() {
    	return attributeAliases;
    }
    
    // if an expression has alias, return its alternate name
    public String getAlias(String expression) {

    	for(String s : attributeAliases.keySet()) {

    		if(s.equalsIgnoreCase(expression)) {
    			return attributeAliases.get(s);
    		}
    		
    	}
    	
    	return null;
    }

    // for debugging
    public void resetAnalyticExpressionIdx() {
    	analyticExpressionIdx = 0;
    }
    
    public AnalyticExpression getAnalyticExpression() {
    	++analyticExpressionIdx; // preincrement
    	return analyticExpressions.get(analyticExpressionIdx-1);
    	
    }
    
    public void resetSortIdx() {
    	sortIdx = 0;
    }
    
    public int getSortIdx() {
    	return sortIdx;
    }
    
    public void setSortIdx(int s) {
    	sortIdx = s;
    }
    
    // have a sort from the query execution plan
    // need to resolve it to OrderByElement to get ASC or DESC clause
    public Sort.SortOrder getSortOrder(List<String> sortKeys) throws Exception {
    	if(sortList == null) {
    		createSortList();
    	}
    	
    	List<OrderByElement> sort = sortList.get(sortIdx);
    	++sortIdx;
    	
    	// make sure they match
    	if(sort.size() != sortKeys.size()) {
    		throw new Exception("Sort keys don't have the same length! " + sort + " " + sortKeys);
    	}
    	
    	    	
   	
    	for(int i = 0; i < sort.size(); ++i) {
    		String expr = sort.get(i).getExpression().toString();
    		String aliasedExpr = attributeAliases.get(expr);
    		String psqlExpr = sortKeys.get(i);
    		
    		
    		if(!expr.equals(psqlExpr) && (aliasedExpr != null && 
    		    !aliasedExpr.equals(psqlExpr))) {
    			throw new Exception("Sort lookup had mismatched keys! Aliased: " + aliasedExpr + ", expression input: " + expr + "!=" + psqlExpr + " (psql expr)");
    		}
    	}
    	
    	if(sort.get(0).isAsc()) {
    		return SortOrder.ASC;
    	}
    	
    	return SortOrder.DESC;
    	
    }
    
    void createSortList() {
    	
    	sortList = new ArrayList<ArrayList<OrderByElement> >();
    	sortIdx = 0;
    	
    	for(int i = 0; i < analyticExpressions.size(); ++i) {
    		// if a sort exists
    		AnalyticExpression ae = analyticExpressions.get(i); 
    		if(ae.getOrderByElements().size() > 0) {
        		
    			// break up by partition by bins first
    			ArrayList<OrderByElement> elements = new ArrayList<OrderByElement>();    			

        		for(Expression e : ae.getPartitionExpressionList().getExpressions()){
        			OrderByElement o = new OrderByElement();
        			o.setExpression(e);
        			elements.add(o);
        			
        		}
        		elements.addAll(ae.getOrderByElements());
        		sortList.add(elements);
    		}
    	}
    	
    	if(orderBy != null) {
    		sortList.add((ArrayList<OrderByElement>) orderBy);
    	}
    	
    }
    
    public String toString() {
    	String s = new String();
    	
    	s = "Table expression has order by " + orderBy + "\n analytic exprs " + analyticExpressions + "\n aliases: " + attributeAliases;
    	
    	return s;
    }
    
    public void setName(String n) {
    	name = n;
    }
    
    public String getName() {
    	return name;
    }
	
}

package istc.bigdawg.plan;


import java.util.HashMap;
import java.util.Map;

import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.operators.*;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;

// keep track of query root node and any CTEs
public class SQLQueryPlan {

	private Map<String, Operator> planRoots;
	private Map<String,SQLTableExpression> tableExpressions; 
	
	private Select statement;
	
	
	Operator rootNode;
	
	public SQLQueryPlan() {
		planRoots = new HashMap<String, Operator>();
		tableExpressions = new HashMap<String, SQLTableExpression>();
	}
	
	
	// produces the composite slice key for the entire query plan
	// consists of the minimal covering set of all nodes, less any private attributes
	public Map<String, SQLAttribute> getSliceKey() {
		// walk the operator tree getting partition slice for each plan node
		// resolve aliases and intermediate results to their sources
		return new HashMap<String, SQLAttribute>();
		
	}
	
	public SQLQueryPlan(Operator root) {
		planRoots = new HashMap<String, Operator>();
		rootNode = root;
	}
	
	public void setRootNode(Operator o) {
		rootNode = o;
	}
	
	// get root of a CTE statement or main
	public Operator getPlanRoot(String statementName) {
		return planRoots.get(statementName);
	}
	
	public String printPlan() {
		String plan = new String();
		
		// prepend plans for chronological order
		for(String s : planRoots.keySet()) {
			plan = "CTE " + s + ": " + planRoots.get(s).printPlan(0) + "\n" + plan;
		}
		plan += rootNode.printPlan(0);
		
		return plan;
	}
	
	
	public Operator getRootNode() {
		return rootNode;
	}
	
	// may be main statement or common table expression
	public void addPlan(String name, Operator root) {
		planRoots.put(name, root);
	}
	
	
	public void setLogicalStatement(Select s) {
		statement = s;
	}
	
	public Select getStatement() {
		return statement;
	}
	
	
	public void addTableSupplement(String name, SQLTableExpression sup) {
		sup.setName(name);
		tableExpressions.put(name, sup);
	}
	
	
	public Map<String, SQLTableExpression> getSupplements() {
		return tableExpressions;
	}

	public SQLTableExpression getSQLTableExpression(String name) {
		return tableExpressions.get(name);
	}
	
	
/*	public void generatePlaintext(Select s) throws JSQLParserException {
		// first cte references a seq scan for the query to have secure input
		for(String cte : planRoots.keySet()) {
			Operator o = planRoots.get(cte);
			WithItem w = new WithItem();
			//o.generatePlaintext((Select) ps, true);
		}

	} */


	public WithItem getWithItem(String localPlan) {
		if(statement.getWithItemsList() == null || statement.getWithItemsList().isEmpty()) {
			return null;
		}

		for(WithItem w : statement.getWithItemsList()) {
			if(w.getName().equals(localPlan)) {
				return w;
			}
		}
		
		return null;
	}

}

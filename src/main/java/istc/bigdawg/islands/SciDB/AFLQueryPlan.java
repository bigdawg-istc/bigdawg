package istc.bigdawg.islands.SciDB;


import java.util.HashMap;
import java.util.Map;

import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.shims.AFLQueryGenerator;
import istc.bigdawg.shims.OperatorQueryGenerator;

// keep track of query root node and any CTEs
public class AFLQueryPlan {

	private Map<String, Operator> planRoots;
//	private Map<String,SQLTableExpression> tableExpressions; 
	
	private String statement;
	
	
	Operator rootNode;
	
	public AFLQueryPlan() {
		planRoots = new HashMap<String, Operator>();
//		tableExpressions = new HashMap<String, SQLTableExpression>();
	}
	
	
	
	public AFLQueryPlan(Operator root) {
		planRoots = new HashMap<String, Operator>();
		rootNode = root;
		root.setQueryRoot(true);
	}
	
	public void setRootNode(Operator o) {
		rootNode = o;
		o.setQueryRoot(true);
	}
	
	// get root of a CTE statement or main
	public Operator getPlanRoot(String statementName) {
		return planRoots.get(statementName);
	}
	
	public String printPlan() throws Exception{
		String plan = new String();
		
		OperatorQueryGenerator gen = new AFLQueryGenerator();
		
		// prepend plans for chronological order
		for(String s : planRoots.keySet()) {
			gen.configure(true,false);
			planRoots.get(s).accept(gen);
			plan = "CTE " + s + ": " + gen.generateStatementString() + "\n" + plan;
		}
		gen.configure(true, false);
		plan += gen.generateStatementString();
		
		return plan;
	}
	
	
	public Operator getRootNode() {
		return rootNode;
	}
	
	// may be main statement or common table expression
	public void addPlan(String name, Operator root) {
		planRoots.put(name, root);
	}
	
	
	public void setLogicalStatement(String s) {
		statement = s;
	}
	
	public String getStatement() {
		return statement;
	}
	
	
//	public void addTableSupplement(String name, SQLTableExpression sup) {
//		sup.setName(name);
//		tableExpressions.put(name, sup);
//	}
//	
//	
//	public Map<String, SQLTableExpression> getSupplements() {
//		return tableExpressions;
//	}
//
//	public SQLTableExpression getSQLTableExpression(String name) {
//		return tableExpressions.get(name);
//	}
//	
	

}

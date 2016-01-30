package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import net.sf.jsqlparser.JSQLParserException;
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
		
		this.joinType = j.joinType;
		this.isCopy = j.isCopy;
		
		if (j.currentWhere == null) this.currentWhere = j.currentWhere;
		else this.currentWhere = new String(j.currentWhere);
		if (j.joinPredicate == null) this.joinPredicate = j.joinPredicate;
		else this.joinPredicate = new String(j.joinPredicate);
		
		
		this.srcSchema = new HashMap<>();
		for (String s : j.srcSchema.keySet()) {
			if (j.srcSchema.get(s) != null) 
				this.srcSchema.put(new String(s), new SQLAttribute(j.srcSchema.get(s)));
		}
	}
	
	public Join(Operator child0, Operator child1, JoinType jt, String joinPred) throws JSQLParserException {
		this.isCTERoot = false; // TODO VERIFY
		this.isBlocking = false; 
		this.isPruned = false;
		this.isCopy = true;
		
		if (jt != null) this.joinType = jt;
		
		if (joinPred != null) {
			this.joinPredicate = joinPred;
			
		}

		this.isQueryRoot = true;
		
		this.dataObjects = new HashSet<>();
		this.joinReservedObjects = new HashSet<>();
		
		this.srcSchema = new LinkedHashMap<String, SQLAttribute>(child0.outSchema);
		srcSchema.putAll(child1.outSchema);
		
		this.outSchema = new LinkedHashMap<String, SQLAttribute>(child0.outSchema);
		outSchema.putAll(child1.outSchema);
		
		
//		Set<String> temp = new HashSet<>();
//		
//		for (String s : child0.getOutSchema().keySet()) {
//			List<String> l = Arrays.asList(child0.getOutSchema().get(s).getFullyQualifiedName().split("\\."));
//			temp.add(l.get(l.size()-1));
//		}
//		
//		for (String s : child1.getOutSchema().keySet()) {
//			List<String> l = Arrays.asList(child1.getOutSchema().get(s).getFullyQualifiedName().split("\\."));
//			if (temp.contains(l.get(l.size()-1))) continue;
//			
//			this.outSchema.put(new String(s), new SQLAttribute(child1.getOutSchema().get(s)));
//		}
		
		this.children = new ArrayList<>();
		this.children.add(child0);
		this.children.add(child1);
		
		if (child0.isCopy()) child0.parent = this;
		if (child1.isCopy()) child1.parent = this;
		
		child0.isQueryRoot = false;
		child1.isQueryRoot = false;
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
 		
 		
 		
 		
 		BinaryExpression on = (BinaryExpression) CCJSqlParserUtil.parseCondExpression(joinPredicate);
		
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
			
 	}
    
    

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
        	Set<String> child0ObjectSet = child0.getDataObjectNames();
        	Set<String> child1ObjectSet = child1.getDataObjectNames();

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
    	
    	
    	
    	// used to find the deepest object
    	Table t = new Table();
    	
    	if (dstStatement == null) {
    		
    		// check if child0 is pruned or one of the scans
    		// if not call child0; 
    		if (child0.isPruned() || child0 instanceof SeqScan || child0 instanceof CommonSQLTableExpressionScan) {

    			if (!(child1.isPruned() || child1 instanceof SeqScan || child1 instanceof CommonSQLTableExpressionScan)) {
    				throw new Exception("child1 of join is not good");
    			}
    			
    			// this is the bottom
    			dstStatement = SelectUtils.buildSelectFromTable(t0);
    			updateThisAndParentJoinReservedObjects(t0.getFullyQualifiedName());
    			
    			t = t1;
    			
    		} else {
    			dstStatement = child0.generatePlaintext(srcStatement, dstStatement); 
    			ps = (PlainSelect) dstStatement.getSelectBody();
    			if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
    			
    			t = t1;
	    		if (this.joinReservedObjects.contains(t1.getFullyQualifiedName())) {
	    			t = t0;
	    		}
    		}
    		
    		
    		
			// leave the rest for child1;
			
		} else {
			
			
			dstStatement = child0.generatePlaintext(srcStatement, dstStatement); 
			ps = (PlainSelect) dstStatement.getSelectBody();
			if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
			
			
			getJoinReservedObjectsFromParents();
			
			
			t = t1;
    		if (this.joinReservedObjects.contains(t1.getFullyQualifiedName())) {
    			t = t0;
    		}
			
		}
    	
		
		// finish with child 1
    	
//    	if (child1 instanceof Join) {
//    	
//			dstStatement = child1.generatePlaintext(srcStatement, dstStatement); 
//			ps = (PlainSelect) dstStatement.getSelectBody();
//			if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
//			
//			if (joinPredicate == null) {
//				addJSQLParserJoin(dstStatement, t); // or the left does not have
//			} else {
//				SelectUtils.addJoin(dstStatement, t, expr);
//			}
//			
//			updateThisAndParentJoinReservedObjects(t.getFullyQualifiedName());
//    	} else {
    		if (joinPredicate == null) addJSQLParserJoin(dstStatement, t);
    		else SelectUtils.addJoin(dstStatement, t, expr);
    		
    		updateThisAndParentJoinReservedObjects(t.getFullyQualifiedName());
    		
    		dstStatement = child1.generatePlaintext(srcStatement, dstStatement); 
    		ps = (PlainSelect) dstStatement.getSelectBody();
    		if (ps.getWhere() != null) filterSet.add(ps.getWhere().toString());
    		
//    	}
    	
    	
		
		
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
    
    
    private void updateThisAndParentJoinReservedObjects(String name) {
    	Operator o = this;
    	o.joinReservedObjects.add(name);
    	while (o.parent != null) {
    		o.parent.joinReservedObjects.add(name);
    		o = o.parent;
    	}
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
	
	public String getCurrentJoinPredicate(){
		return joinPredicate;
	};
};

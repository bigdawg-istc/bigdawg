package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;

public class Operator {

	
	private boolean isCTERoot = false;
	// for use in getPlaintext


	// does this op need access to all inputs before it can emit output?
	// e.g., max, min, sort
	// these block force sync points in our setup
	protected boolean isBlocking; 
	
	
	// consider adding this:
	// denotes that filter, scan, and any joins on replicas can be computed locally
	// no potential to do remote coordination
	protected boolean isLocal; 
	
	
	
	// denoting sliced execution by op (e.g., comorbidity query)
	protected Map<String, SQLAttribute> outSchema;
	
	
	// direct descendants
	protected List<Operator> children;


	protected Operator parent = null;
	
	
	
	protected final int indent = 4;
	
	
	
	protected boolean isPruned = false;
	protected static int pruneCount = 0;
	protected Integer pruneID = null;
	
	protected boolean isQueryRoot = false;
	
	
	public Operator(Map<String, String> parameters, List<String> output,  
			Operator child, // this is changed to 
			SQLTableExpression supplement) {

		
		
		// order preserving
		outSchema = new LinkedHashMap<String, SQLAttribute>();
		children  = new ArrayList<Operator>();
		

		
		
		if(child != null) { // check for leaf nodes
			children.add(child);
			child.setParent(this);
			
		}
		
		
		// if it is a subplan, add it to the ctes list -- moved out to planparser
		/* if(parameters.containsKey("Subplan-Name")) {
			String planName = parameters.get("Subplan-Name");
			planName = planName.substring(planName.indexOf(" "));
			plan.addCommonSQLTableExpression(planName, this);
		} */
		
	}

	
	public Operator(Map<String, String> parameters, List<String> output, 
			Operator lhs, Operator rhs,
			SQLTableExpression supplement) {
		
		outSchema = new LinkedHashMap<String, SQLAttribute>();
		children  = new ArrayList<Operator>();
		
		children.add(lhs);
		children.add(rhs);

		lhs.setParent(this);
		rhs.setParent(this);

		// if it is a subplan, add it to the ctes list -- moved out to plan parser
		/* if(parameters.containsKey("Subplan-Name")) {
			String planName = parameters.get("Subplan-Name");
			planName = planName.substring(planName.indexOf(" "));
			plan.addCommonSQLTableExpression(planName, this);
		}*/
		

	}
	
	public Operator() {
		
	}
	
	public Operator(Operator o) throws Exception {
		if (!this.getClass().equals(o.getClass())) throw new Exception();
		
		this.isCTERoot = o.isCTERoot;
		this.isBlocking = o.isBlocking; 
		this.isLocal = o.isLocal; 
		this.isPruned = o.isPruned;
		this.pruneID = o.pruneID;

		this.isQueryRoot = o.isQueryRoot;
		
		this.outSchema = new LinkedHashMap<>();
		for (String s : o.outSchema.keySet()) {
			this.outSchema.put(new String(s), new SQLAttribute(o.outSchema.get(s)));
		}
		
		this.children = new ArrayList<>();
		for (Operator s : o.children) {
			if (s instanceof Join) {
				Join j = new Join(s);
				j.setParent(this);
				children.add(j);
			} else if (s instanceof SeqScan) {
				SeqScan ss = new SeqScan(s);
				ss.setParent(this);
				children.add(ss);
			} else if (s instanceof CommonSQLTableExpressionScan) {
				CommonSQLTableExpressionScan c = new CommonSQLTableExpressionScan(s);
				c.setParent(this);
				children.add(c);
			} else if (s instanceof Sort) {
				Sort t = new Sort(s);
				t.setParent(this);
				children.add(t);
			} else {
				if (s instanceof Aggregate) {
				} else if (s instanceof Distinct) {
				} else if (s instanceof WindowAggregate) {
				} else {
					throw new Exception("Unknown Operator from Operator Copy: "+s.getClass().toString());
				}
				throw new Exception("Unsupported Operator Copy: "+s.getClass().toString());
			}
		}
	}

	public boolean CTERoot() {
		return isCTERoot;
	}

	public void setCTERootStatus(boolean b) {
		isCTERoot = b;
	}
	
	public void setParent(Operator p) {
		parent = p;
	}
	
	public Operator getParent() {
		return parent;
	}
	

	public List<Operator> getChildren() {
		return children;
	}
	
	
	
	
	
	// composed of this and everything below it on the dag
	public List<String> getCompositeSliceNames(SQLQueryPlan q) throws JSQLParserException {
		Set<SQLAttribute> sliceKeys = new HashSet<SQLAttribute>();
		
		sliceKeys = this.getCompositeSliceKey(sliceKeys);
		
		List<String> attrList = new ArrayList<String>();
		for(SQLAttribute s : sliceKeys) {
			attrList.add(s.getFullyQualifiedName());
		}
		

		
		return attrList;
	}
	

	public List<SQLAttribute> getCompositeSliceKeys(SQLQueryPlan q) throws JSQLParserException {
		Set<SQLAttribute> sliceKeys = new HashSet<SQLAttribute>();
		
		sliceKeys = this.getCompositeSliceKey(sliceKeys);
		
		List<SQLAttribute> attrList = new ArrayList<SQLAttribute>();
		for(SQLAttribute s : sliceKeys) {
			attrList.add(s);
		}
		
		return attrList;

	}
	
	
	
	protected Set<SQLAttribute> getCompositeSliceKey(Set<SQLAttribute> keys) throws JSQLParserException {
		addSliceKeys(this, keys);

		for(Operator c : children) {
			keys = c.getCompositeSliceKey(keys);
		}
		if(this instanceof CommonSQLTableExpressionScan) {
			CommonSQLTableExpressionScan cte = (CommonSQLTableExpressionScan) this;
			Operator source = cte.getSourceStatement();
			keys =  source.getCompositeSliceKey(keys);
		}

		return keys;
	}
	
	
	
	private void addSliceKeys(Operator o, Set<SQLAttribute> keys) throws JSQLParserException {
		List<SQLAttribute> l = o.getSliceKey(); 
		if(l != null) {
			for(SQLAttribute s : l) {
				if(s.getSourceAttributes() == null) {
					keys.add(s);
				}
				else { 
					for(SQLAttribute t : s.getSourceAttributes()) {
						keys.add(t);  // record their provenance
					}
 				}
			}
		}
		
	}

	// seems like this will be overridden
	public List<SQLAttribute> getSliceKey() throws JSQLParserException {
		return null;
	}
	
	
	public void addChild(Operator aChild) {
		children.add(aChild);
	}

	public void addChilds(List<Operator> childs) {
		children.addAll(childs);
	}
	
	
	public String generateSelectForExecutionTree(Select srcStatement, String into) throws Exception {
		
		Select dstStatement  = this.generatePlaintext(srcStatement, null);
//		System.out.println("PLAIN DSTSTATEMENT: "+ ((PlainSelect) dstStatement.getSelectBody()));
		
		// iterate over out schema and add it to select clause
		List<SelectItem> selects = new ArrayList<SelectItem>();

		for(String s : outSchema.keySet()) {
			SQLAttribute attr = new SQLAttribute(outSchema.get(s));
			
			changeAttributeName(attr);
			
			SelectExpressionItem si = new SelectExpressionItem(attr.getExpression());
			if(!(si.toString().equals(attr.getName()))) {
				si.setAlias(new Alias(attr.getName()));
			}
			
			selects.add(si);
		}
		
		((PlainSelect) dstStatement.getSelectBody()).setSelectItems(selects);
		
		
		// dealing with WITH statment
		if (into != null) {
			
			if (dstStatement.getWithItemsList() == null) {
				return addInto(dstStatement.getSelectBody(), into);
			}
			
			for (WithItem wi : dstStatement.getWithItemsList()) {
				if (wi.getName().equals(into)) {
					return addInto(wi.getSelectBody(), into);
				}
			}

			return addInto(dstStatement.getSelectBody(), into);
		}
		return ((PlainSelect) dstStatement.getSelectBody()).toString();
	}
	
	/**
	 * add an SELECT INTO token to the select body
	 * @param body
	 * @param into
	 * @return the resulting text
	 */
	private String addInto(SelectBody body, String into) {
		Table t = new Table();
		t.setName(into);
		ArrayList<Table> tl = new ArrayList<>();
		tl.add(t);
		
		((PlainSelect) body).setIntoTables(tl);
		return ((PlainSelect) body).toString();
	}
	
	/**
	 * This Function updates the attribute name to match with that of the referenced table
	 * @param attr
	 * @return
	 * @throws Exception
	 */
	public boolean changeAttributeName(SQLAttribute attr) throws Exception {
		// if no children, then do nothing 
		// for each children, 
		// check if the child is pruned, 
		//     if so check if it bears the name; 
		//         if so, change the attribute name 

		if (children.size() > 0) {
			for (Operator o : children) {
				if (o.isPruned()) {
					if (o.getOutSchema().containsKey(attr.getFullyQualifiedName())) {
						Expression e = attr.getExpression();
						
						if (e instanceof Column) {
							Table t = new Table();
							t.setName(o.getPruneToken());
							
							Column newE = new Column(t, ((Column)e).getColumnName());
							
							attr.setName(newE.toString());
							attr.setExpression(newE);
						} else {
							throw new Exception ("Unsupported column type: "+e.getClass().toString());
						}
						
						return true;
					}
				} else {
					if ( o.changeAttributeName(attr) ) return true;
				}
			}
		}
		
		return false;
	}
	
	// this is the implicit root of the SQL generated
	public String generatePlaintext(Select srcStatement) throws Exception {
		
		Select dstStatement  = this.generatePlaintext(srcStatement, null);
		
		
		// iterate over out schema and add it to select clause
		List<SelectItem> selects = new ArrayList<SelectItem>();

		for(String s : outSchema.keySet()) {
			SQLAttribute attr = new SQLAttribute(outSchema.get(s));
						
			// find the table where it is pruned
			changeAttributeName(attr);
			
			SelectExpressionItem si = new SelectExpressionItem(attr.getExpression());
			if(!(si.toString().equals(attr.getName()))) {
				si.setAlias(new Alias(attr.getName()));
			}
			
			selects.add(si);
		}
		
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		ps.setSelectItems(selects);
		
		return dstStatement.toString();

	}
	// each operator adds its parts to this to regenerate that 
	// part of the query for plaintext execution
	//  tail recursion through query plan
	// isRoot denotes the root node in a CTE or main select statement
	
	// srcStatement = entire statement initially submitted by user
	// dstStatement builds a statement that potentially contains a subset of the nodes depending on where plaintext stops in SQL execution
	protected Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {

		// generic case
		for(int i = 0; i < children.size(); ++i) {
			dstStatement = children.get(i).generatePlaintext(srcStatement, dstStatement);
		}
		return dstStatement;
	}
	
	protected static void addSelectItem(Expression expr, List<SelectItem> selects) {
		boolean found = false;
		for(SelectItem s : selects) {
			if(s.toString().equalsIgnoreCase(expr.toString())) {
				found = true;
			}
		}
		
		if(!found) {
			selects.add((SelectItem) expr);
		}
	}
	

	
	// recurse through plan and print it in nested form
	// each op adds its part
	// produces an plan similar to SciDB's AFL syntax
	public String printPlan(int recursionLevel) {
		return new String();
	}
	
	protected static String padLeft(String s, int n) {
		if(n > 0) {
			return String.format("%1$" + n + "s", s);  
		}
		 
		return s;
	}

	
	public Map<String, SQLAttribute>  getOutSchema() {
		return outSchema;
	}


	// find any filters associated with a slice key
	// takes in any existing filters
	// s must be derived from just one table
	// otherwise slice key will not be computable without distributed join

	// TODO: extend this to CTE scans, for each comparison check source attributes
	// if they all belong to source attribute's table add them to the predicate
	
	
	public String getScanPredicates(SQLAttribute s) throws Exception {
		
		String table = null;
		Set<SQLAttribute> sources = s.getSourceAttributes();
		if(sources == null) {
			table = s.getTable();
		}
		else {
			for(SQLAttribute attr : sources) {
				if(table == null) {
					table = attr.getTable();
				}
				else {
					if(!attr.getTable().equals(table)) {
						throw new Exception("Cannot derive slice predicate from greater than one table!");
					}
				}
				
			}
			
		}
		
		
		return getScanPredicatesHelper(table);
			
	}		
		
	private String getScanPredicatesHelper(String tableName) {
		if(this instanceof SeqScan) {
			SeqScan scan = ( SeqScan) this;
			String localName = scan.srcTable;
			if(tableName.equals(localName)) {
				return scan.filterExpression;
			}
			return null;
		}
		
		String ret = new String();
		for(Operator c : children) {
			String cPred = c.getScanPredicatesHelper(tableName);
			if(cPred != null) {
				ret = ret + cPred;
			}
		}
		
		if(ret.length() > 0) {
			return ret;
		}
		// no predicates found
		return null;
	}
	
	// if it is blocking, this operator changes our SMC control flow
	// e.g., c-diff can't complete part of the self-join locally because it has a sort that needs to run first.
	// other joins can do partial matches locally
	// can keep passing around partial results until we hit a blocking operator
	// trace the path of the data through plan nodes
	public boolean blockingStatus() {
		return isBlocking;
	}
	
	
	/**
	 * NOTE: MODIFY THIS SO IT UPDATES MAP WITH PRUNE INFORMATION
	 * getLocation gets a list of result locations that are possible for this operator
	 * @return List<Integer> of dbid
	 */
	public Map<String, ArrayList<String>> getTableLocations(Map<String, ArrayList<String>> map) {
		Map<String, ArrayList<String>> result = new HashMap<>();
		for (Operator o : children) {
			result.putAll(o.getTableLocations(map));
		}
		return result;
	}
	
	public boolean isPruned() {
		return isPruned;
	}
	
	public void prune(boolean p) {
		if (p && this.pruneID == null) {
			pruneCount += 1;
			this.pruneID = pruneCount;
		}
		isPruned = p;
	}
	
	public String getPruneToken() throws Exception {
		
		if (!isPruned) {
			throw new Exception("\n\n\n----> unpruned token: "+this.outSchema.toString()+"\n\n");
		}
		
		return "BIGDAWGPRUNED_"+this.pruneID;
	}
	
	public void setQueryRoot() {
		this.isQueryRoot = true;
	}
	
	public boolean isQueryRoot() {
		return this.isQueryRoot;
	}
}

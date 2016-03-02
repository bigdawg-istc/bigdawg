package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.WithItem;

public class Operator {

	
	protected boolean isCTERoot = false;
	// for use in getPlaintext

	
	// does this op need access to all inputs before it can emit output?
	// e.g., max, min, sort
	// these block force sync points in our setup
	protected boolean isBlocking; 
	protected static int blockerCount = 0;
	protected Integer blockerID = null;
	
	
	// denoting sliced execution by op (e.g., comorbidity query)
	protected Map<String, DataObjectAttribute> outSchema;
	
	
	// direct descendants
	protected List<Operator> children;
	protected Operator parent = null;
	
	
	protected boolean isPruned = false;
	protected static int pruneCount = 0;
	protected Integer pruneID = null;
	
	protected boolean isQueryRoot = false;
	
	
	protected Set<String> dataObjects;
	protected Set<String> joinReservedObjects;
	protected boolean isCopy = false;  // used in building permutations; only remainder join operators could attain true, so far
	
	
	public Operator(Map<String, String> parameters, List<String> output,  
			Operator child, // this is changed to 
			SQLTableExpression supplement) {

		
		
		// order preserving
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();

		
		
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

	// for AFL
	public Operator(Map<String, String> parameters, SciDBArray output,  
			Operator child) {

		
		
		// order preserving
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();
		
		
		if(child != null) { // check for leaf nodes
			children.add(child);
			child.setParent(this);
			
		}
		
	}
	
	public Operator(Map<String, String> parameters, List<String> output, 
			Operator lhs, Operator rhs,
			SQLTableExpression supplement) {
		
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();
		
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
	
	// for AFL
	public Operator(Map<String, String> parameters, SciDBArray output, 
			Operator lhs, Operator rhs) {
		
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();
		
		children.add(lhs);
		children.add(rhs);

		lhs.setParent(this);
		rhs.setParent(this);

	}
	
	public Operator() {
		
	}
	
	public Operator(Operator o, boolean addChild) throws Exception {
		
		this.isCTERoot = o.isCTERoot;
		this.isBlocking = o.isBlocking; 
		this.isPruned = o.isPruned;
		this.pruneID = o.pruneID;

		this.isQueryRoot = o.isQueryRoot;
		
		this.dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();
		
		
		this.outSchema = new LinkedHashMap<>();
		for (String s : o.outSchema.keySet()) {
			
			if (o.outSchema.get(s) instanceof SQLAttribute) {
				this.outSchema.put(new String(s), new SQLAttribute((SQLAttribute)o.outSchema.get(s)));
			} else {
				this.outSchema.put(new String(s), new DataObjectAttribute(o.outSchema.get(s)));
			}
		}
		
		this.children = new ArrayList<>();
		
		
		if (addChild) {
			for (Operator s : o.children) {
				
				// pruned nodes are not regenerated
				if (s.isPruned()) {
					this.children.add(s);
					continue;
				}
				
				Operator op = s.duplicate(addChild);
				op.setParent(this);
				this.children.add(op);
				
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
	
	
	
	public void addChild(Operator aChild) {
		children.add(aChild);
	}

	public void addChilds(List<Operator> childs) {
		children.addAll(childs);
	}
	
	
	public String generateSQLSelectIntoStringForExecutionTree(String into) throws Exception {
		
		Select dstStatement  = this.generateSQLStringDestOnly(null, false);
//		System.out.println("PLAIN DSTSTATEMENT: "+ ((PlainSelect) dstStatement.getSelectBody()));
		
		// iterate over out schema and add it to select clause
		HashMap<String, SelectItem> selects = new HashMap<String, SelectItem>();

		for(String s : outSchema.keySet()) {
			SQLAttribute attr = new SQLAttribute((SQLAttribute)outSchema.get(s));
			
			changeAttributeName(attr);
			
			SelectExpressionItem si = new SelectExpressionItem(attr.getExpression());
			if(!(si.toString().equals(attr.getName()))) {
				si.setAlias(new Alias(attr.getName()));
			}
			
			selects.put(s, si);
		}
		
		
		// this might be important TODO 
//		((PlainSelect) dstStatement.getSelectBody()).setSelectItems(changeSelectItemsOrder(srcStatement, selects));
		
		
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
	
	
	
	public String generateAFLStoreStringForExecutionTree(String into) throws Exception {
		
		String dstString = this.generateAFLString(1); // this gets rid of "scan" if there is one
		
		if (into != null) {
			dstString = "store(" + dstString + ", " + into + ")";
		}
		
		return dstString;
	}
	
	
	private List<SelectItem> changeSelectItemsOrder(Select srcStatement, HashMap<String, SelectItem> selects) throws Exception {
		List<SelectItem> orders = ((PlainSelect) srcStatement.getSelectBody()).getSelectItems();
		List<SelectItem> holder = new ArrayList<>();
		
		SelectItemVisitor siv = new SelectItemVisitor() {

			@Override
			public void visit(AllColumns allColumns) {
				holder.add(allColumns);
			}

			@Override
			public void visit(AllTableColumns allTableColumns) {
				holder.add(allTableColumns);
			}

			@Override
			public void visit(SelectExpressionItem selectExpressionItem) {

				// find the child where the pruned token or seqscan or CTE is located, make it the corresponding position
				
				Column c = (Column) selectExpressionItem.getExpression();
				String out = c.getFullyQualifiedName();
				if (selects.get(out) != null)
					holder.add(selects.get(out));
				else if (selects.get(out = c.getTable().getName()+ "."+ c.getColumnName()) != null)
					holder.add(selects.get(out));
				else {
					out = c.getFullyQualifiedName();
					// well.
					for (String s : selects.keySet()) {
						if (s.endsWith(out)){
							holder.add(selects.get(s));
							break;
						}
					}
				}
				selects.remove(out);
			}
			
		};
		
		
		for (SelectItem si : orders) {
			si.accept(siv);
		}
		return holder;
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
					if (o.getOutSchema().containsKey(attr.getName())) {
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
	
	
	protected Select generateSQLStringDestOnly(Select dstStatement, boolean stopAtJoin) throws Exception {

		// generic case
		for(int i = 0; i < children.size(); ++i) {
			dstStatement = children.get(i).generateSQLStringDestOnly(dstStatement, stopAtJoin);
		}
		return dstStatement;
	}
	
	
	/**
	 * The bulk of work for generating SQL statement
	 * 
	 * @param srcStatement, used to reorder select items, place 'null' if order of SelectItems not important
	 * @return dstStatement
	 * @throws Exception
	 */
	private Select prepareForSQLGeneration(Select srcStatement) throws Exception {

		clearJoinReservedObjects();
		Select dstStatement  = this.generateSQLStringDestOnly(null, false);
		
		// iterate over out schema and add it to select clause
		HashMap<String, SelectItem> selects = new HashMap<String, SelectItem>();

		for(String s : outSchema.keySet()) {
			SQLAttribute attr = new SQLAttribute((SQLAttribute)outSchema.get(s));

			// find the table where it is pruned
			changeAttributeName(attr);
			
			SelectExpressionItem si = new SelectExpressionItem(attr.getExpression());
			if(!(si.toString().equals(attr.getName()))) {
				si.setAlias(new Alias(attr.getFullyQualifiedName()));
			}
			
			selects.put(s, si);
		}
		
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		
		if (srcStatement != null)
			ps.setSelectItems(changeSelectItemsOrder(srcStatement, selects));
		
		return dstStatement;
	}
	
	
	// this is the implicit root of the SQL generated
	public String generateSQLString(Select srcStatement) throws Exception {
		
		Select dstStatement = prepareForSQLGeneration(srcStatement);
		return dstStatement.toString();

	}
	
	public String generateSQLWithWidthBucket(String widthBucketString, String into, Select srcStatement) throws Exception {
		
		Select dstStatement = prepareForSQLGeneration(srcStatement);
		
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		
		String newWhere = "(" + ps.getWhere().toString() + ") AND ("+widthBucketString+")";
		ps.setWhere(CCJSqlParserUtil.parseCondExpression(newWhere));
		
		
		if (into != null) 
			addInto(ps, into); 
		
		return dstStatement.toString();
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
	public String generateAFLString(int recursionLevel) throws Exception {
		return new String();
	}
	
	
	
	public Map<String, DataObjectAttribute>  getOutSchema() {
		return outSchema;
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
	 * @return List<String> of dbid
	 */
	public Map<String, List<String>> getTableLocations(Map<String, List<String>> map) {
		Map<String, List<String>> result = new HashMap<>();
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
	
	
	public void getJoinReservedObjectsFromParents() {
		if (parent != null) {
			this.joinReservedObjects.addAll(this.parent.joinReservedObjects);
		}
	}
	
	
	public Set<String> getDataObjectNames() throws Exception {
		
		if (isPruned) {
			Set<String> temps = new HashSet<>();
			temps.add(getPruneToken());
			return temps;
		}
		
		if (!(this instanceof SeqScan || this instanceof CommonSQLTableExpressionScan)) {
			this.dataObjects.clear();
			for (Operator o : children) {
				this.dataObjects.addAll(o.getDataObjectNames());
			}
		}
		
		return dataObjects;
	}
	
	public List<Operator> getDataObjects() {
		List<Operator> extraction = new ArrayList<>();
		
		if (isPruned) {
			extraction.add(this);
			return extraction;
		}
		
		if (!(this instanceof SeqScan || this instanceof CommonSQLTableExpressionScan)) {
			for (Operator o : children) {
				extraction.addAll(o.getDataObjects());
			}
		} else {
			extraction.add(this);
		}
		
		
		return extraction;
	}
	
	public boolean isCopy(){
		return this.isCopy;
	};
	
	
	public List<Operator> getAllBlockers(){
		
		List<Operator> extraction = new ArrayList<>();
		
		if (isBlocking) {
			extraction.add(this);
			return extraction;
		}
		
		for (Operator c : children) {
			extraction.addAll(c.getAllBlockers());
		}
		
		
		return extraction;
	}
	
	public Integer getBlockerID() throws Exception {
		if (!isBlocking)
			throw new Exception("Operator Not blocking: "+this.toString());
		return blockerID;
	}
	
	private void clearJoinReservedObjects() {
		this.joinReservedObjects.clear();
		for (Operator c : children)
			c.clearJoinReservedObjects();
	}
	
	public Operator duplicate(boolean addChild) throws Exception {
		if (this instanceof Join) {
			return new Join(this, addChild);
		} else if (this instanceof SeqScan) {
			return new SeqScan(this, addChild);
		} else if (this instanceof CommonSQLTableExpressionScan) {
			return new CommonSQLTableExpressionScan(this, addChild);
		} else if (this instanceof Sort) {
			return new Sort(this, addChild);
		} else {
			throw new Exception("Unsupported Operator Copy: "+this.getClass().toString());
		}
	}
	
	// will likely get overridden
	public String getTreeRepresentation(boolean isRoot){
		return "Unimplemented: "+this.getClass().toString();
	}
	
	
	public String generateSQLCreateTableStatementLocally(String name){
		StringBuilder sb = new StringBuilder();
		
		sb.append("CREATE TABLE ").append(name).append(' ').append('(');
		
		boolean started = false;
		
		for (DataObjectAttribute doa : outSchema.values()) {
			if (started == true) sb.append(',');
			else started = true;
			
			sb.append(doa.generateSQLTypedString());
		}
		
		sb.append(')');
		
		return sb.toString();
	} 
	
	public String generateAFLCreateArrayStatementLocally(String name){
		StringBuilder sb = new StringBuilder();
		
		List<DataObjectAttribute> attribs = new ArrayList<>();
		List<DataObjectAttribute> dims = new ArrayList<>();
		
		for (DataObjectAttribute doa : outSchema.values()) {
			if (doa.isHidden()) dims.add(doa);
			else attribs.add(doa);
		}
		
		
		sb.append("CREATE ARRAY ").append(name).append(' ').append('<');
		
		boolean started = false;
		for (DataObjectAttribute doa : attribs) {
			if (started == true) sb.append(',');
			else started = true;
			
			sb.append(doa.generateAFLTypeString());
		}
		
		sb.append('>').append('[');
		if (dims.isEmpty()) {
			sb.append("i=0:*,10000000,0");
		} else {
			started = false;
			for (DataObjectAttribute doa : dims) {
				if (started == true) sb.append(',');
				else started = true;
				
				sb.append(doa.generateAFLTypeString());
			}
		}
		sb.append(']');
		
		return sb.toString();
	} 
	
	
	public Operator generateSQLStatementForPresentNonJoinSegment(StringBuilder sb) throws Exception {
		
		if ( ! this.getClass().equals(Join.class)) {
			sb.append(this.generateSQLStringDestOnly(null, true).toString()); 
		}

		// find the join		
		Operator child = this;
		while (child != null && (!child.getClass().equals(Join.class))) {
			// then there could be one child only
			child = child.getChildren().get(0);
		}
		
		return child;
	}
}

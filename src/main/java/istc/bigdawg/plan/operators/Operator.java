package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLExpressionHandler;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.SelectUtils;

public class Operator {

	
	protected boolean isCTERoot = false;
	// for use in getPlaintext

	
	// does this op need access to all inputs before it can emit output?
	// e.g., max, min, sort
	// these block force sync points in our setup
	protected boolean isBlocking = false; 
	protected static int blockerCount = 0;
	protected Integer blockerID = null;
	
	protected boolean isPruned = false;
	protected static int pruneCount = 0;
	protected Integer pruneID = null;
	
	protected boolean isSubTree = false;
	protected static int subTreeCount = 0;
	protected Integer subTreeID = null;
	
	
	protected Map<String, DataObjectAttribute> outSchema;
	
	protected Map<String, String> complexOutItemFromProgeny;
	
	
	// direct descendants
	protected List<Operator> children;
	protected Operator parent = null;
	
	protected boolean isQueryRoot = false;
	
	
	protected Set<String> dataObjects;
	protected Set<String> joinReservedObjects;
	protected Set<String> objectAliases = null;
	protected boolean isCopy = false;  // used in building permutations; only remainder join operators could attain true, so far
	
	
	// SQL, single non sort
	public Operator(Map<String, String> parameters, List<String> output,  
			Operator child, // this is changed to 
			SQLTableExpression supplement) {

		
		
		// order preserving
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		complexOutItemFromProgeny = new LinkedHashMap<>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();

		
		if(child != null) { // check for leaf nodes
			children.add(child);
			child.setParent(this);
			
			populateComplexOutItem(true);
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
		complexOutItemFromProgeny = new HashMap<>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();
		
		
		if(child != null) { // check for leaf nodes
			children.add(child);
			child.setParent(this);
			
		}
		
	}
	
	
	// SQL, join
	public Operator(Map<String, String> parameters, List<String> output, 
			Operator lhs, Operator rhs,
			SQLTableExpression supplement) {
		
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		complexOutItemFromProgeny = new LinkedHashMap<>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		joinReservedObjects = new HashSet<>();
		
		children.add(lhs);
		children.add(rhs);

		lhs.setParent(this);
		rhs.setParent(this);
		
		populateComplexOutItem(true);
		

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
		this.joinReservedObjects = new HashSet<>();
		this.complexOutItemFromProgeny = new LinkedHashMap<>();
		
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

	protected void populateComplexOutItem(boolean first) {
		// populate complexOutItemFromProgeny
		
		if (!first) {
			
//			System.out.printf("\n\npopulate false: %s\n\n", this.getClass().getSimpleName());
			
			complexOutItemFromProgeny = new LinkedHashMap<>();
			
			List<Operator> walker = new ArrayList<>();
			walker.addAll(children);
			while (!walker.isEmpty()) {
				List<Operator> nextgen = new ArrayList<>();
				for (Operator child : walker){
					if (child.isPruned() || child instanceof Join) {
						for (String s: child.getOutSchema().keySet()) {
							Expression e = child.getOutSchema().get(s).getSQLExpression();
							if (e == null) continue;
							while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
							if (e instanceof Column) continue;
							
							System.out.printf("\n\n\n====> found string: %s: \n\n\n", e);
							
							complexOutItemFromProgeny.put(s, e.toString().replaceAll("[.]", "\\[\\.\\]").replaceAll("[(]", "\\[\\(\\]").replaceAll("[)]", "\\[\\)\\]"));
						}
					} else {
//						if (child.complexOutItemFromProgeny.isEmpty()) child.populateComplexOutItem(false);
						nextgen.addAll(child.children);
						continue;
					}
					
					complexOutItemFromProgeny.putAll(child.complexOutItemFromProgeny);
				}
				walker = nextgen;
			}
		} else {
			for (Operator child : children){
				for (String s: child.getOutSchema().keySet()) {
					Expression e = child.getOutSchema().get(s).getSQLExpression();
					if (e == null) continue;
					while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
					if (e instanceof Column) continue;
					complexOutItemFromProgeny.put(s, e.toString().replaceAll("[.]", "\\[\\.\\]").replaceAll("[(]", "\\[\\(\\]").replaceAll("[)]", "\\[\\)\\]"));
				}
				complexOutItemFromProgeny.putAll(child.complexOutItemFromProgeny);
			}
		}
	}
	
	protected String rewriteComplextOutItem(String expr) throws Exception {
		// simplify
		expr = CCJSqlParserUtil.parseExpression(expr).toString();
		for (String alias : complexOutItemFromProgeny.keySet()) {
			expr = expr.replaceAll("("+complexOutItemFromProgeny.get(alias)+")", alias);
		}
		return expr;
	}
	
	protected String rewriteComplextOutItem(Expression e) throws Exception {
		// simplify
		String expr = e.toString();
		for (String alias : complexOutItemFromProgeny.keySet()) {
			expr = expr.replaceAll("("+complexOutItemFromProgeny.get(alias)+")", alias);
		}
		return expr;
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
	
	
	public String generateSQLSelectIntoStringForExecutionTree(String into, boolean stopAtJoin) throws Exception {
		Select dstStatement  = prepareForSQLGeneration(null, stopAtJoin);
		return addSelectIntoToken(dstStatement, into);
	}
	
	public String addSelectIntoToken(Select dstStatement, String into) {
		// dealing with WITH statment
		if (into != null) {
			
			if (dstStatement.getWithItemsList() == null) {
				addInto(dstStatement.getSelectBody(), into);
				return dstStatement.toString();
			}
			
			// single out the with statement 
			for (WithItem wi : dstStatement.getWithItemsList()) {
				if (wi.getName().equals(into)) {
					addInto(wi.getSelectBody(), into);
					dstStatement.getWithItemsList().remove(wi); // remove this item so it no longer gets bundled
					return wi.getSelectBody().toString();
				}
			}
		}
		return dstStatement.toString();
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
				
				Expression e = selectExpressionItem.getExpression();
				
				
				SQLExpressionHandler deparser = new SQLExpressionHandler() {
					@Override
					public void visit(Column tableColumn) {
						String out = tableColumn.getFullyQualifiedName();
						if (selects.get(out) != null)
							holder.add(selects.get(out));
						else if (selects.get(out = tableColumn.getTable().getName()+ "."+ tableColumn.getColumnName()) != null)
							holder.add(selects.get(out));
						else {
							out = tableColumn.getFullyQualifiedName();
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
					
					@Override
					public void visit(Parenthesis parenthesis) {
						parenthesis.getExpression().accept(this);
					}
					
					@Override
					public void visit(Function function) {
						holder.add(selectExpressionItem);
					}
					
					@Override
					protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
						holder.add(selectExpressionItem);
					}
				};
				
				e.accept(deparser);
				
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
	private void addInto(SelectBody body, String into) {
		Table t = new Table();
		t.setName(into);
		ArrayList<Table> tl = new ArrayList<>();
		tl.add(t);
		
		PlainSelect ps = (PlainSelect) body; 
		ps.setIntoTables(tl);
		
		
		List<String> columnNames = new ArrayList<>();
		List<SelectItem> seli = new ArrayList<>();
		SelectItemVisitor siv = new SelectItemVisitor() {
			@Override public void visit(AllColumns allColumns) {}
			@Override public void visit(AllTableColumns allTableColumns) {} // this is bad, but we can't do too much about it TODO
			@Override
			public void visit(SelectExpressionItem selectExpressionItem) {
				try {
					List<String> columns = SQLExpressionUtils.getColumnNamesInAllForms(selectExpressionItem.getExpression());
					if (!columns.removeAll(columnNames)) {
						columnNames.addAll(columns); // TODO EXTREMELY BAD PRACTICE. BETTER SOLUTION NEEDED
						seli.add(selectExpressionItem);
					}
					
				} catch (JSQLParserException e) {e.printStackTrace();}
			}
		};
		for (SelectItem si : ps.getSelectItems()) si.accept(siv);
		ps.setSelectItems(seli);
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

		Expression e = attr.getSQLExpression();
		Set<Column> attribsExpr = new HashSet<>(SQLExpressionUtils.getAttributes(e));
		Set<String> attribs = new HashSet<>();
		
		for (Column c : attribsExpr) attribs.add(c.getFullyQualifiedName());
		
		boolean ret = false;
		
		if (children.size() > 0) {
			for (Operator o : children) {
				if (o.isPruned()) {
					
					if (o.getOutSchema().containsKey(attr.getName())) {
						
						Set<String> replacementSet = new HashSet<String>(this.objectAliases);
						replacementSet.add(attr.getName());
						SQLExpressionUtils.renameAttributes(e, replacementSet, null, o.getPruneToken());
						
						attr.setExpression(e.toString());
						
						return true;
					} else if (attribs.removeAll(o.getOutSchema().keySet())) {
						
						Set<String> replacementSet = new HashSet<String>(this.objectAliases);
						replacementSet.add(attr.getName());
						SQLExpressionUtils.renameAttributes(e, replacementSet, null, o.getPruneToken());
						
						attr.setExpression(e.toString());
						ret = true;
					}
				} else if (o instanceof Aggregate && ((Aggregate)o).aggregateID != null) {
					
					
					// TODO REDO: change the entire expression to match the output of children aggregates 
					// TODO SOLVE DUPLICATION PROBLEM
					
					if (o.getOutSchema().containsKey(attr.getName()) || attribs.removeAll(o.getOutSchema().keySet())) {
						
						if (e instanceof Column) ((Column)e).setTable(new Table(((Aggregate)o).getAggregateToken()));
						else e = new Column(new Table(((Aggregate)o).getAggregateToken()), attr.getName());
						
						attr.setExpression(e.toString());
						ret = true;
						break;
					}
				} else {
					if ( o.changeAttributeName(attr) ) return true;
				}
			}
		}
		
		return ret;
	}
	
	
	protected Select generateSQLStringDestOnly(Select dstStatement, boolean isSubTreeRoot, boolean stopAtJoin, Set<String> allowedScans) throws Exception {
		
		// generic case
		for (Operator o : children)
			dstStatement = o.generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans);
		return dstStatement;
	}
	
	
	/**
	 * The bulk of work for generating SQL statement
	 * 
	 * @param srcStatement, used to reorder select items, place 'null' if order of SelectItems not important
	 * @return dstStatement
	 * @throws Exception
	 */
	private Select prepareForSQLGeneration(Select srcStatement, boolean stopAtJoin) throws Exception {

		clearJoinReservedObjects();
		boolean originalPruneStatus = this.isPruned();
		this.prune(false);
		Select dstStatement  = this.generateSQLStringDestOnly(null, true, stopAtJoin, this.getDataObjectAliasesOrNames().keySet());
		this.prune(originalPruneStatus);
		
		// iterate over out schema and add it to select clause
		HashMap<String, SelectItem> selects = new HashMap<String, SelectItem>();

		updateObjectAliases(false);
		changeAttributesForSelectItems(srcStatement, (PlainSelect) dstStatement.getSelectBody(), selects);
		
//		if (isSubTreeRoot == true && ) stopAtJoin = true;
		return postProcGenSQLStopJoin(dstStatement, stopAtJoin);
	}
	
	protected boolean isAnyProgenyPruned() {
		if (this.isPruned) return true;
		else if (children.isEmpty()) return false;
		else return children.get(0).isAnyProgenyPruned();
	}
	
	private void changeAttributesForSelectItems(Select srcStatement, PlainSelect ps, HashMap<String, SelectItem> selects) throws Exception {
		List<SelectItem> selectItemList = new ArrayList<>(); 
		
		for(String s : outSchema.keySet()) {
			SQLAttribute attr = new SQLAttribute((SQLAttribute)outSchema.get(s));

			// find the table where it is pruned
			changeAttributeName(attr);
			
			SelectExpressionItem si = new SelectExpressionItem(attr.getSQLExpression());
			
			if(!(si.toString().equals(attr.getName())) && !(attr.getSQLExpression() instanceof Column)) {
				si.setAlias(new Alias(attr.getFullyQualifiedName()));
			}
			
			if (srcStatement == null)
				selectItemList.add(si);
			else 
				selects.put(s, si);
		}
		
		if (srcStatement == null)
			ps.setSelectItems(selectItemList);
		else 
			ps.setSelectItems(changeSelectItemsOrder(srcStatement, selects));
		
		if (ps.getFromItem() instanceof SubSelect) {
			changeAttributesForSelectItems(null, (PlainSelect)((SubSelect)ps.getFromItem()).getSelectBody(), selects);
		}
		if (ps.getJoins() != null) {
			for (net.sf.jsqlparser.statement.select.Join j : ps.getJoins()) {
				if (j.getRightItem() instanceof SubSelect)
					changeAttributesForSelectItems(null, (PlainSelect)((SubSelect)j.getRightItem()).getSelectBody(), selects);
			}
		} 
	}
	
	
	// this is the implicit root of the SQL generated
	public String generateSQLString(Select srcStatement) throws Exception {
		
		Select dstStatement = prepareForSQLGeneration(srcStatement, false);
		return dstStatement.toString();

	}
	
	public String generateSQLWithWidthBucket(String widthBucketString, String into, Select srcStatement) throws Exception {
		
		Select dstStatement = prepareForSQLGeneration(srcStatement, false);
		
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
			throw new Exception("\n\n\n----> unpruned token: "+this.outSchema+"\n\n");
		}
		
		return "BIGDAWGPRUNED_"+this.pruneID;
	}
	
	public boolean isSubTree() {
		return this.isSubTree;
	}
	
	public void setSubTree(boolean t) {
		
		if (this instanceof Join) return;
		
		if (t && this.subTreeID == null) {
			subTreeCount += 1;
			this.subTreeID = subTreeCount;
		}
		isSubTree = t;
	}
	
	public String getSubTreeToken() throws Exception {
		
		if (!isSubTree && !(this instanceof Join)) {
			throw new Exception("\n\n\n----> not the root of a SubTree: "+this.outSchema+"\n\n");
		}
		
		if (this instanceof Join) return ((Join)this).getJoinToken(); 
		else if (this instanceof Aggregate && ((Aggregate)this).aggregateID != null) return ((Aggregate)this).getAggregateToken();
		else return "BIGDAWGSUBTREE_"+this.subTreeID;
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
		
		if (!(this instanceof Scan)) {
			this.dataObjects.clear();
			for (Operator o : children) {
				this.dataObjects.addAll(o.getDataObjectNames());
			}
		}
		
		return dataObjects;
	}
	
	public Map<String, String> getDataObjectAliasesOrNames() throws Exception {
		
		Map<String, String> aliasOrString = new LinkedHashMap<>();
		
		if (isSubTree) {
			aliasOrString.put(getSubTreeToken(), getSubTreeToken());
//			return aliasOrString;
		}
		
		if (isPruned) {
			aliasOrString.put(getPruneToken(), getPruneToken());
//			return aliasOrString;
		}
		
		if (this instanceof Aggregate && ((Aggregate)this).aggregateID != null)
			aliasOrString.put(((Aggregate)this).getAggregateToken(), ((Aggregate)this).getAggregateToken());
		
		if (this.children.size() > 0 ) {
			
			for (Operator o : children) {
				aliasOrString.putAll(o.getDataObjectAliasesOrNames());
			}
		} else {
			if (((Scan)this).tableAlias != null && !((Scan)this).tableAlias.isEmpty())
				aliasOrString.put(((Scan)this).tableAlias, ((Scan)this).table.getFullyQualifiedName());
			else 
				aliasOrString.put(((Scan)this).srcTable, ((Scan)this).table.getFullyQualifiedName());
		}
		
		return aliasOrString;
	}
	
	public List<Operator> getDataObjects() {
		List<Operator> extraction = new ArrayList<>();
		
		if (isPruned) {
			extraction.add(this);
			return extraction;
		}
		
		if (!(this instanceof Scan )) {
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
		} else if (this instanceof Aggregate) {
			return new Aggregate(this, addChild);
		} else if (this instanceof Limit) {
			return new Limit(this, addChild);
		} else if (this instanceof Distinct) {
			return new Distinct(this, addChild);
		} else {
			throw new Exception("Unsupported Operator Copy: "+this.getClass().toString());
		}
	}
	
	
	
	public void updateObjectAliases(boolean lazy) {
		
		if (lazy && this.objectAliases != null)
			return;
		
		objectAliases = new HashSet<String>();
		if (this instanceof Scan && ((Scan)this).tableAlias != null) {
			objectAliases.add(((Scan)this).tableAlias);
		} else if (this instanceof Join) {
			
			children.get(1).updateObjectAliases(lazy);
			objectAliases.addAll(children.get(1).objectAliases);
		} 

		if (children.size() != 0) {
			children.get(0).updateObjectAliases(lazy);
			objectAliases.addAll(children.get(0).objectAliases);
		}
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
	
	
	public Join generateSQLStatementForPresentNonJoinSegment(StringBuilder sb, boolean isSelect) throws Exception {
		
		// find the join		
		Operator child = this;
		while (!(child instanceof Join) && !child.getChildren().get(0).isPruned()) 
			// then there could be one child only
			child = child.getChildren().get(0);
		
		Select outputSelect;
		
		if ( !(this instanceof Join) && (child instanceof Join)) {
			// TODO targeted strike? CURRENTLY WASH EVERYTHING // Set<String> names = child.getDataObjectNames();
			outputSelect 		= this.generateSQLStringDestOnly(null, true, true, this.getDataObjectAliasesOrNames().keySet());
			
			Map<String, String> ane				= this.getChildren().get(0).getDataObjectAliasesOrNames();
			Set<String> childAliases			= ane.keySet();
			Set<String> childAliasesAndNames	= new HashSet<>(ane.values());
			for (String s : ane.values()) childAliasesAndNames.add(s);
			
			populateComplexOutItem(false);
			
			PlainSelect ps = ((PlainSelect)outputSelect.getSelectBody());
			List<SelectItem> sil = ps.getSelectItems();
			for (int i = 0 ; i < sil.size(); i ++) {
				if (sil.get(i) instanceof SelectExpressionItem) {
					SelectExpressionItem sei = (SelectExpressionItem)sil.get(i);
					sei.setExpression(CCJSqlParserUtil.parseExpression(rewriteComplextOutItem(sei.getExpression())));
				}
			}
			
//			System.out.printf("\n\n\n---> childAlias&Names: %s\n\n\n", childAliasesAndNames);
			String token;
			if (child.isPruned()) token = child.getPruneToken();
    		else token = ((Join)child).getJoinToken();
				
			updateSubTreeTokens(ps, childAliases, childAliasesAndNames, token);
			if (outputSelect.getWithItemsList() != null) 
				for (WithItem wi : outputSelect.getWithItemsList())
					updateSubTreeTokens(((PlainSelect)wi.getSelectBody()), childAliases, childAliasesAndNames, token);
			
			this.setSubTree(true);
			if (!isSelect) addSelectIntoToken(outputSelect, this.getSubTreeToken());
			
			sb.append(outputSelect);
		} else if (!(this instanceof Join) && !(child instanceof Join)) {
			outputSelect = this.generateSQLStringDestOnly(null, true, true, this.getDataObjectAliasesOrNames().keySet());
			
			this.setSubTree(true);
			if (!isSelect) addSelectIntoToken(outputSelect, this.getSubTreeToken());
			
			throw new Exception ("---->> shouldn't be here: "+outputSelect);
			
//			sb.append(outputSelect);
		} 
		
		if (child instanceof Join)
			return (Join) child;
		else 
			return null;
	}
	
	protected void updateSubTreeTokens(PlainSelect ps, Set<String> originalAliases, Set<String> aliasesAndNames, String subTreeToken) throws Exception {
		List<OrderByElement> obes 	= ps.getOrderByElements();
		List<Expression> gbes 		= ps.getGroupByColumnReferences();
		List<SelectItem> sis 		= ps.getSelectItems();
		Expression where = ps.getWhere();
		Expression having = ps.getHaving();
		
		// CHANGE WHERE AND HAVING
		if (where != null) SQLExpressionUtils.renameAttributes(where, originalAliases, aliasesAndNames, subTreeToken);
		if (having != null) SQLExpressionUtils.renameAttributes(having, originalAliases, aliasesAndNames, subTreeToken);
		
		// CHANGE ORDER BY
		if (obes != null && !obes.isEmpty()) 
			for (OrderByElement obe : obes) 
				SQLExpressionUtils.renameAttributes(obe.getExpression(), originalAliases, aliasesAndNames, subTreeToken);
		
		// CHANGE GROUP BY and SELECT ITEM
		if (gbes != null && !gbes.isEmpty()) {
			for (Expression gbe : gbes) 
				SQLExpressionUtils.renameAttributes(gbe, originalAliases, aliasesAndNames, subTreeToken);
		}
		for (SelectItem si : sis) {
			SelectItemVisitor siv = new SelectItemVisitor() {
				@Override public void visit(AllColumns allColumns) {}
				@Override public void visit(AllTableColumns allTableColumns) {}
				@Override public void visit(SelectExpressionItem selectExpressionItem) {
					try {
						SQLExpressionUtils.renameAttributes(selectExpressionItem.getExpression(), originalAliases, aliasesAndNames, subTreeToken);
					} catch (JSQLParserException e) {e.printStackTrace();}}};
			si.accept(siv);
		}
		
		// CHANGE FROM AND JOINS
		
		FromItemVisitor fv = new FromItemVisitor() {
			@Override public void visit(Table tableName) {}
			@Override public void visit(ValuesList valuesList) {}
			@Override public void visit(SubJoin subjoin) {subjoin.getLeft().accept(this);}
			@Override public void visit(LateralSubSelect lateralSubSelect) {lateralSubSelect.getSubSelect().accept(this);}

			@Override
			public void visit(SubSelect subSelect) {
				try { 
					updateSubTreeTokens((PlainSelect)subSelect.getSelectBody(), originalAliases, aliasesAndNames, subTreeToken);
				} catch (Exception e) { e.printStackTrace(); }
			}
		};
		
		ps.getFromItem().accept(fv);
		if (ps.getJoins() != null)
			for (net.sf.jsqlparser.statement.select.Join j : ps.getJoins())
				j.getRightItem().accept(fv);
		
	}
	
	protected Select postProcGenSQLStopJoin(Select dstStatement, boolean stopAtJoin) throws Exception {
    	
    	if (dstStatement == null || !stopAtJoin) return dstStatement;
    	
    	for (Operator o : children) {
    		Operator child = o;
    		while ((!child.getChildren().isEmpty()) && (!child.getClass().equals(Join.class))) child = child.getChildren().get(0);
    		
    		String token;
    		
    		if (child.isPruned()) token = child.getPruneToken();
    		else token = ((Join)child).getJoinToken();
    		
    		if (!child.children.isEmpty()) {
    			Map<String, String> ane = child.getDataObjectAliasesOrNames();
    			Set<String> childAliases = ane.keySet();
    			Set<String> childAliasesAndNames = new HashSet<>(ane.keySet());
    			for (String s : ane.values()) childAliasesAndNames.add(s);
    			updateSubTreeTokens(((PlainSelect)dstStatement.getSelectBody()), childAliases, childAliasesAndNames, token);
//    			if (dstStatement.getWithItemsList() != null)
//    				for (WithItem wi : dstStatement.getWithItemsList())
//    					updateJoinTokens(((PlainSelect)wi.getSelectBody()), childNames, ((Join)child).getJoinToken());
    		}
    	}
    	
    	
    	return dstStatement;
    }
	
	// will likely get overridden
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		throw new Exception("Unimplemented: "+this.getClass().toString());
	}
	
	// half will be overriden
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		return children.get(0).getObjectToExpressionMappingForSignature();
	}
	
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) {
		for (Operator o: children)
			o.removeCTEEntriesFromObjectToExpressionMapping(entry);
	}
	
	/**
	 * Serves only getObjectToExpressionMappingForSignature()
	 * @param e
	 * @param out
	 * @throws Exception 
	 */
	protected void addToOut(Expression e, Map<String, Set<String>> out, Map<String, String> aliasMapping) throws Exception {
		
		while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
		SQLExpressionUtils.restoreTableNamesFromAliasForSignature(e, aliasMapping);
		
		
		Set<String> names = SQLExpressionUtils.getAllTableNamesForSignature(e, aliasMapping);
		for (String n : names) {
			
			if (out.get(n) == null) {
				Set<String> addition = new HashSet<>();
				addition.add(e.toString());
				out.put(n, addition);
			} else {
				out.get(n).add(e.toString());
			}
		}
	}
	
	public Expression resolveAggregatesInFilter(String e, boolean goParent, Operator lastHopOp, Set<String> names, StringBuilder sb) throws Exception {
		
		if (goParent && parent != null) 
			return parent.resolveAggregatesInFilter(e, goParent, this, names, sb);
		else if (!goParent) {
			Expression exp = null;
			for (Operator o : children) {
				if (o == lastHopOp) break;
				if ((exp = o.resolveAggregatesInFilter(e, goParent, this, names, sb)) != null) return exp;
			}
		} 
		
		return null;
	} 
	
	public void seekScanAndProcessAggregateInFilter() throws Exception {
		for (Operator o : children) 
			o.seekScanAndProcessAggregateInFilter();
	}
	
	protected Map<String, Expression> getChildrenIndexConds() throws Exception {
		return this.getChildren().get(0).getChildrenIndexConds();
	}
	
	protected Select generateSelectWithToken(String token) throws Exception {
    	Select dstStatement = SelectUtils.buildSelectFromTable(new Table(token));
		PlainSelect ps = (PlainSelect)dstStatement.getSelectBody();
		List<SelectItem> lsi = new ArrayList<>();
		for (String s : outSchema.keySet()) {
			SelectExpressionItem sei = new SelectExpressionItem();
			Expression e = CCJSqlParserUtil.parseExpression(outSchema.get(s).getExpressionString());
			SQLExpressionUtils.renameAttributes(e, null, null, token);
			sei.setExpression(e);
			lsi.add(sei);
		}
		ps.setSelectItems(lsi);
		return dstStatement;
    }
	
	public void updateOutSchema(Map<String, DataObjectAttribute> schema) throws JSQLParserException {
		Map<String, DataObjectAttribute> update = new HashMap<>();
		for (String s: schema.keySet()) {
			if (schema.get(s) instanceof SQLAttribute) update.put(new String(s), new SQLAttribute((SQLAttribute)schema.get(s)));
			else update.put(new String(s), new DataObjectAttribute(schema.get(s)));
		}
		this.outSchema = update;
	}
}

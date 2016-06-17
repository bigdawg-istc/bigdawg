package istc.bigdawg.islands.SciDB.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.utils.SQLExpressionUtils;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.OperatorInterface;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.SelectUtils;

public class SciDBIslandOperator implements OperatorInterface, Operator {

	
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
	
	private Map<String, String> complexOutItemFromProgeny;
	
	
	// direct descendants
	protected List<Operator> children;
	protected Operator parent = null;
	
	protected boolean isQueryRoot = false;
	
	
	protected Set<String> dataObjects;
	private Set<String> objectAliases = null;
	protected boolean isCopy = false;  // used in building permutations; only remainder join operators could attain true, so far
	
//	
//	// SQL, single non sort non union
//	public SciDBIslandOperator(Map<String, String> parameters, List<String> output,  
//			SciDBIslandOperator child, // this is changed to 
//			SQLTableExpression supplement) {
//
//		
//		// order preserving
//		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
//		setComplexOutItemFromProgeny(new LinkedHashMap<>());
//		children  = new ArrayList<Operator>();
//		dataObjects = new HashSet<>();
//		
//		if(child != null) { // check for leaf nodes
//			children.add(child);
//			child.setParent(this);
//			
//			populateComplexOutItem(true);
//		}
//		
//		
//		
//		// if it is a subplan, add it to the ctes list -- moved out to planparser
//		/* if(parameters.containsKey("Subplan-Name")) {
//			String planName = parameters.get("Subplan-Name");
//			planName = planName.substring(planName.indexOf(" "));
//			plan.addCommonSQLTableExpression(planName, this);
//		} */
//		
//	}

	// for AFL
	public SciDBIslandOperator(Map<String, String> parameters, SciDBArray output,  
			Operator child) {

		
		
		// order preserving
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		setComplexOutItemFromProgeny(new HashMap<>());
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		
		if(child != null) { // check for leaf nodes
			children.add(child);
			child.setParent(this);
			
		}
		
	}
	
	
//	// SQL, join
//	public SciDBIslandOperator(Map<String, String> parameters, List<String> output, 
//			SciDBIslandOperator lhs, SciDBIslandOperator rhs,
//			SQLTableExpression supplement) {
//		
//		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
//		setComplexOutItemFromProgeny(new LinkedHashMap<>());
//		children  = new ArrayList<Operator>();
//		dataObjects = new HashSet<>();
//		
//		children.add(lhs);
//		children.add(rhs);
//
//		lhs.setParent(this);
//		rhs.setParent(this);
//		
//		populateComplexOutItem(true);
//		
//
//		// if it is a subplan, add it to the ctes list -- moved out to plan parser
//		/* if(parameters.containsKey("Subplan-Name")) {
//			String planName = parameters.get("Subplan-Name");
//			planName = planName.substring(planName.indexOf(" "));
//			plan.addCommonSQLTableExpression(planName, this);
//		}*/
//		
//
//	}
	
	// for AFL
	public SciDBIslandOperator(Map<String, String> parameters, SciDBArray output, 
			Operator lhs, Operator rhs) {
		
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
//		joinReservedObjects = new HashSet<>();
		
		children.add(lhs);
		children.add(rhs);

		lhs.setParent(this);
		rhs.setParent(this);

	}
	
	
//	// SQL, UNION
//	public SciDBIslandOperator(Map<String, String> parameters, List<String> output,  
//			List<SciDBIslandOperator> childs, SQLTableExpression supplement) {
//
//		// order preserving
//		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
//		setComplexOutItemFromProgeny(new LinkedHashMap<>());
//		children  = new ArrayList<Operator>();
//		dataObjects = new HashSet<>();
//		
//		children.addAll(childs);
//		for (SciDBIslandOperator c : childs) c.setParent(this);
////		populateComplexOutItem(true);
//		
//		
//		// if it is a subplan, add it to the ctes list -- moved out to planparser
//		/* if(parameters.containsKey("Subplan-Name")) {
//			String planName = parameters.get("Subplan-Name");
//			planName = planName.substring(planName.indexOf(" "));
//			plan.addCommonSQLTableExpression(planName, this);
//		} */
//		
//	}
	
	// for AFL UNION
	public SciDBIslandOperator(Map<String, String> parameters, SciDBArray output,  
			List<Operator> childs) {

		// order preserving
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		setComplexOutItemFromProgeny(new HashMap<>());
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
		
		for (Operator o : childs) { // check for leaf nodes
			children.add(o);
			o.setParent(this);
			
		}
		
	}
	
	public SciDBIslandOperator() {
		
	}
	
	public SciDBIslandOperator(SciDBIslandOperator o, boolean addChild) throws Exception {
		
		this.isCTERoot = o.isCTERoot;
		this.isBlocking = o.isBlocking; 
		this.isPruned = o.isPruned;
		this.pruneID = o.pruneID;

		this.isQueryRoot = o.isQueryRoot;
		
		this.dataObjects = new HashSet<>();
		this.setComplexOutItemFromProgeny(new LinkedHashMap<>());
		
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

//	protected void populateComplexOutItem(boolean first) {
//		// populate complexOutItemFromProgeny
//		for (Operator child : children){
//			if ((!first) && child.getComplexOutItemFromProgeny().isEmpty()) child.populateComplexOutItem(first);
//			for (String s: child.getOutSchema().keySet()) {
//				Expression e = child.getOutSchema().get(s).getSQLExpression();
//				if (e == null) continue;
//				while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
//				if (e instanceof Column) continue;
//				getComplexOutItemFromProgeny().put(s, e.toString().replaceAll("[*]", "\\[\\*\\]").replaceAll("[.]", "\\[\\.\\]").replaceAll("[(]", "\\[\\(\\]").replaceAll("[)]", "\\[\\)\\]"));
//			}
//			if (first || (!child.isPruned()
//					|| (child instanceof SciDBIslandJoin && ((SciDBIslandJoin)child).getJoinID() == null)
//					|| (child instanceof SciDBIslandAggregate && ((SciDBIslandAggregate)child).getAggregateID() == null))) getComplexOutItemFromProgeny().putAll(child.getComplexOutItemFromProgeny());
//		}
//	}
	
	protected String rewriteComplextOutItem(String expr) throws Exception {
		// simplify
		expr = CCJSqlParserUtil.parseExpression(expr).toString();
		for (String alias : getComplexOutItemFromProgeny().keySet()) {
			expr = expr.replaceAll("("+getComplexOutItemFromProgeny().get(alias)+")", alias);
		}
		return expr;
	}
	
	protected String rewriteComplextOutItem(Expression e) throws Exception {
		// simplify
		String expr = e.toString();
		for (String alias : getComplexOutItemFromProgeny().keySet()) {
			expr = expr.replaceAll(getComplexOutItemFromProgeny().get(alias), alias);
		}
		return expr;
	}
	
	@Override
	public boolean isCTERoot() {
		return isCTERoot;
	}

	@Override
	public void setCTERoot(boolean b) {
		isCTERoot = b;
	}
	
	@Override
	public void setParent(Operator p) {
		parent = p;
	}
	
	@Override
	public Operator getParent() {
		return parent;
	}

	@Override
	public List<Operator> getChildren() {
		return children;
	}
	
	@Override
	public void addChild(Operator aChild) {
		children.add(aChild);
	}

	@Override
	public void addChilds(Collection<Operator> childs) {
		children.addAll(childs);
	}
		
//	public String generateAFLStoreStringForExecutionTree(String into) throws Exception {
//		
//		String dstString = this.generateAFLString(1); // this gets rid of "scan" if there is one
//		
//		if (into != null) {
//			dstString = "store(" + dstString + ", " + into + ")";
//		}
//		
//		return dstString;
//	}
	
	
	public boolean isAnyProgenyPruned() {
		if (this.isPruned) return true;
		else if (children.isEmpty()) return false;
		else return ((SciDBIslandOperator) children.get(0)).isAnyProgenyPruned();
	}
	
	// recurse through plan and print it in nested form
	// each op adds its part
	// produces an plan similar to SciDB's AFL syntax
//	public String generateAFLString(int recursionLevel) throws Exception {
//		return new String();
//	}
	
	
	
	public Map<String, DataObjectAttribute>  getOutSchema() {
		return outSchema;
	}
	
	// if it is blocking, this operator changes our SMC control flow
	// e.g., c-diff can't complete part of the self-join locally because it has a sort that needs to run first.
	// other joins can do partial matches locally
	// can keep passing around partial results until we hit a blocking operator
	// trace the path of the data through plan nodes
	public boolean isBlocking() {
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
			result.putAll(((SciDBIslandOperator) o).getTableLocations(map));
		}
		return result;
	}
	
	@Override
	public boolean isPruned() {
		return isPruned;
	}
	
	@Override
	public void prune(boolean p) {
		if (p && this.pruneID == null) {
			pruneCount += 1;
			this.pruneID = pruneCount;
		}
		isPruned = p;
	}
	
	@Override
	public String getPruneToken() throws Exception {
		if (!isPruned) 
			throw new Exception("\n\n\n----> unpruned token: "+this.outSchema+"\n\n");
		return BigDAWGPruneToken + this.pruneID;
	}
	
	@Override
	public boolean isSubTree() {
		return this.isSubTree;
	}
	
	@Override
	public void setSubTree(boolean t) {
		if (this instanceof SciDBIslandJoin) return;
		if (t && this.subTreeID == null) {
			subTreeCount += 1;
			this.subTreeID = subTreeCount;
		}
		isSubTree = t;
	}
	
	@Override
	public String getSubTreeToken() throws Exception {
		if (!isSubTree && !(this instanceof SciDBIslandJoin)) return null;
		if (this instanceof SciDBIslandJoin) return ((SciDBIslandJoin)this).getJoinToken(); 
		else if (this instanceof SciDBIslandAggregate && ((SciDBIslandAggregate)this).getAggregateID() != null) return ((SciDBIslandAggregate)this).getAggregateToken();
		else return BigDAWGSubtreeToken + this.subTreeID;
	}
	
	@Override
	public void setQueryRoot(boolean isRoot) {
		this.isQueryRoot = isRoot;
	}
	
	@Override
	public boolean isQueryRoot() {
		return this.isQueryRoot;
	}
	
	@Override
	public Map<String, String> getDataObjectAliasesOrNames() throws Exception {
		
		Map<String, String> aliasOrString = new LinkedHashMap<>();
		
		boolean masked = false;
		String replaceToken = null;
		
		if (isSubTree) {
			replaceToken = getSubTreeToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (isPruned) {
			replaceToken = getPruneToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
			
		}
		
		if (this instanceof SciDBIslandJoin && ((SciDBIslandJoin)this).joinID != null) {
			replaceToken = ((SciDBIslandJoin)this).getJoinToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (this instanceof SciDBIslandAggregate && ((SciDBIslandAggregate)this).getAggregateID() != null) {
			replaceToken = ((SciDBIslandAggregate)this).getAggregateToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (this.children.size() > 0 ) {
			
			if (this instanceof SciDBIslandScan) {
				if (((SciDBIslandScan)this).getTableAlias() != null && !((SciDBIslandScan)this).getTableAlias().isEmpty())
					aliasOrString.put(((SciDBIslandScan)this).getTableAlias(), ((SciDBIslandScan)this).table.getFullyQualifiedName());
				else 
					aliasOrString.put(((SciDBIslandScan)this).getSourceTableName(), ((SciDBIslandScan)this).table.getFullyQualifiedName());
			}
			
			for (Operator o : children) {
				aliasOrString.putAll(o.getDataObjectAliasesOrNames());
				if (masked) {
					for (String s : o.getDataObjectAliasesOrNames().keySet()) {
						aliasOrString.replace(s, replaceToken);
					}
				}
			}
		} else {
			if (((SciDBIslandScan)this).getTableAlias() != null && !((SciDBIslandScan)this).getTableAlias().isEmpty())
				aliasOrString.put(((SciDBIslandScan)this).getTableAlias(), ((SciDBIslandScan)this).table.getFullyQualifiedName());
			else 
				aliasOrString.put(((SciDBIslandScan)this).getSourceTableName(), ((SciDBIslandScan)this).table.getFullyQualifiedName());
		}
		
		return aliasOrString;
	}
	
	public List<Operator> getDataObjects() {
		List<Operator> extraction = new ArrayList<>();
		
		if (isPruned) {
			extraction.add(this);
			return extraction;
		}
		
		if (!(this instanceof SciDBIslandScan )) {
			for (Operator o : children) {
				extraction.addAll(((SciDBIslandOperator) o).getDataObjects());
			}
		} else {
			extraction.add(this);
		}
		
		
		return extraction;
	}
	
	@Override
	public boolean isCopy(){
		return this.isCopy;
	};
	
	@Override
	public Integer getBlockerID() throws Exception {
		if (!isBlocking)
			throw new Exception("SciDBIslandOperator Not blocking: "+this.toString());
		return blockerID;
	}
	
	@Override
	public Operator duplicate(boolean addChild) throws Exception {
		if (this instanceof SciDBIslandJoin) {
			return new SciDBIslandJoin(this, addChild);
		} else if (this instanceof SciDBIslandSeqScan) {
			return new SciDBIslandSeqScan(this, addChild);
		} else if (this instanceof SciDBIslandSort) {
			return new SciDBIslandSort(this, addChild);
		} else if (this instanceof SciDBIslandAggregate) {
			return new SciDBIslandAggregate(this, addChild);
//		} else if (this instanceof SciDBIslandLimit) {
//			return new SciDBIslandLimit(this, addChild);
		} else if (this instanceof SciDBIslandDistinct) {
			return new SciDBIslandDistinct(this, addChild);
		} else if (this instanceof SciDBIslandMerge) {
			return new SciDBIslandMerge (this, addChild);
		} else {
			throw new Exception("Unsupported SciDBIslandOperator Copy: "+this.getClass().toString());
		}
	}
	
	
	
	public void updateObjectAliases() {
		
		setObjectAliases(new HashSet<String>());
		if (this instanceof SciDBIslandScan && ((SciDBIslandScan)this).getTableAlias() != null) {
			getObjectAliases().add(((SciDBIslandScan)this).getTableAlias());
		} else if (this instanceof SciDBIslandJoin) {
			
			((SciDBIslandOperator) children.get(1)).updateObjectAliases();
			getObjectAliases().addAll(((SciDBIslandOperator) children.get(1)).getObjectAliases());
		} 

		if (children.size() != 0) {
			((SciDBIslandOperator) children.get(0)).updateObjectAliases();
			getObjectAliases().addAll(((SciDBIslandOperator) children.get(0)).getObjectAliases());
		}
	}
	
	
//	public String generateCreateStatementLocally(String name){
//		StringBuilder sb = new StringBuilder();
//		
//		sb.append("CREATE TABLE ").append(name).append(' ').append('(');
//		
//		boolean started = false;
//		
//		for (DataObjectAttribute doa : outSchema.values()) {
//			if (started == true) sb.append(',');
//			else started = true;
//			
//			sb.append(doa.generateSQLTypedString());
//		}
//		
//		sb.append(')');
//		
//		return sb.toString();
//	} 
	
//	public String generateAFLCreateArrayStatementLocally(String name){
//		StringBuilder sb = new StringBuilder();
//		
//		List<DataObjectAttribute> attribs = new ArrayList<>();
//		List<DataObjectAttribute> dims = new ArrayList<>();
//		
//		for (DataObjectAttribute doa : outSchema.values()) {
//			if (doa.isHidden()) dims.add(doa);
//			else attribs.add(doa);
//		}
//		
//		
//		sb.append("CREATE ARRAY ").append(name).append(' ').append('<');
//		
//		boolean started = false;
//		for (DataObjectAttribute doa : attribs) {
//			if (started == true) sb.append(',');
//			else started = true;
//			
//			sb.append(doa.generateAFLTypeString());
//		}
//		
//		sb.append('>').append('[');
//		if (dims.isEmpty()) {
//			sb.append("i=0:*,10000000,0");
//		} else {
//			started = false;
//			for (DataObjectAttribute doa : dims) {
//				if (started == true) sb.append(',');
//				else started = true;
//				
//				sb.append(doa.generateAFLTypeString());
//			}
//		}
//		sb.append(']');
//		
//		return sb.toString();
//	} 
	
	
	// will likely get overridden
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		throw new Exception("Unimplemented: "+this.getClass().toString());
	}
	
	// half will be overriden
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		return children.get(0).getObjectToExpressionMappingForSignature();
	}
	
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) {
		for (Operator o: children)
			((SciDBIslandOperator) o).removeCTEEntriesFromObjectToExpressionMapping(entry);
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
	
	public Expression resolveAggregatesInFilter(String e, boolean goParent, SciDBIslandOperator lastHopOp, Set<String> names, StringBuilder sb) throws Exception {
		
		if (goParent && parent != null) 
			return ((SciDBIslandOperator)parent).resolveAggregatesInFilter(e, goParent, this, names, sb);
		else if (!goParent) {
			Expression exp = null;
			for (Operator o : children) {
				if (o == lastHopOp) break;
				if ((exp = ((SciDBIslandOperator) o).resolveAggregatesInFilter(e, goParent, this, names, sb)) != null) return exp;
			}
		} 
		
		return null;
	} 
	
	public void seekScanAndProcessAggregateInFilter() throws Exception {
		for (Operator o : children) 
			((SciDBIslandOperator)o).seekScanAndProcessAggregateInFilter();
	}
	
	public Map<String, Expression> getChildrenPredicates() throws Exception {
		return ((SciDBIslandOperator) this.getChildren().get(0)).getChildrenPredicates();
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

	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}

	public Set<String> getObjectAliases() {
		return objectAliases;
	}

	public void setObjectAliases(Set<String> objectAliases) {
		this.objectAliases = objectAliases;
	}

	public Map<String, String> getComplexOutItemFromProgeny() {
		return complexOutItemFromProgeny;
	}

	public void setComplexOutItemFromProgeny(Map<String, String> complexOutItemFromProgeny) {
		this.complexOutItemFromProgeny = complexOutItemFromProgeny;
	}

	@Override
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


	@Override
	public Integer getPruneID() {
		if (isPruned) return pruneID;
		else return null;
	}
}

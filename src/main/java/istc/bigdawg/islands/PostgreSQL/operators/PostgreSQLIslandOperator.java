package istc.bigdawg.islands.PostgreSQL.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.PostgreSQL.utils.SQLAttribute;
import istc.bigdawg.islands.PostgreSQL.utils.SQLExpressionUtils;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.OperatorInterface;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.SelectUtils;

public class PostgreSQLIslandOperator implements Operator, OperatorInterface {

	
//	protected boolean isCTERoot = false;
//	// for use in getPlaintext
//
//	
//	// does this op need access to all inputs before it can emit output?
//	// e.g., max, min, sort
//	// these block force sync points in our setup
//	protected boolean isBlocking = false; 
//	protected static int blockerCount = 0;
//	protected Integer blockerID = null;
//	
//	protected boolean isPruned = false;
//	protected static int pruneCount = 0;
//	protected Integer pruneID = null;
//	
//	protected boolean isSubTree = false;
//	protected static int subTreeCount = 0;
//	protected Integer subTreeID = null;
//	
	
	protected boolean isBlocking = false; 
	protected static int blockerCount = 0;
	protected Integer blockerID = null;
	
	protected boolean isPruned = false;
	protected static int pruneCount = 0;
	protected Integer pruneID = null;
	
	protected boolean isSubTree = false;
	protected static int subTreeCount = 0;
	protected Integer subTreeID = null;
	
	protected boolean isQueryRoot = false;
	protected boolean isCTERoot = false;
	protected List<Operator> children;

	protected Operator parent = null;
	protected boolean isCopy = false;
	
	protected Map<String, DataObjectAttribute> outSchema;
	
	private Map<String, String> complexOutItemFromProgeny;
	
	
	
	protected Set<String> dataObjects;
	private Set<String> objectAliases = null;
//	protected boolean isCopy = false;  // used in building permutations; only remainder join operators could attain true, so far
	
	
	// SQL, single non sort non union
	public PostgreSQLIslandOperator(Map<String, String> parameters, List<String> output,  
			PostgreSQLIslandOperator child, // this is changed to 
			SQLTableExpression supplement) {

		
		// order preserving
		this();
//		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
//		setComplexOutItemFromProgeny(new LinkedHashMap<>());
//		children  = new ArrayList<Operator>();
//		dataObjects = new HashSet<>();
		
		if(child != null) { // check for leaf nodes
			children.add(child);
			child.setParent(this);
			
			populateComplexOutItem(true);
		}
		
	}

	
	// SQL, join
	public PostgreSQLIslandOperator(Map<String, String> parameters, List<String> output, 
			PostgreSQLIslandOperator lhs, PostgreSQLIslandOperator rhs,
			SQLTableExpression supplement) {
		
		this();
//		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
//		setComplexOutItemFromProgeny(new LinkedHashMap<>());
//		children  = new ArrayList<Operator>();
//		dataObjects = new HashSet<>();
		
		children.add(lhs);
		children.add(rhs);

		lhs.setParent(this);
		rhs.setParent(this);
		
		populateComplexOutItem(true);
		
	}
	
	// SQL, UNION
	public PostgreSQLIslandOperator(Map<String, String> parameters, List<String> output,  
			List<PostgreSQLIslandOperator> childs, SQLTableExpression supplement) {

		// order preserving
		this();
//		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
//		setComplexOutItemFromProgeny(new LinkedHashMap<>());
//		children  = new ArrayList<Operator>();
//		dataObjects = new HashSet<>();
		
		children.addAll(childs);
		for (PostgreSQLIslandOperator c : childs) c.setParent(this);
		populateComplexOutItem(true);
		
	}
	
	
	public PostgreSQLIslandOperator() {
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		setComplexOutItemFromProgeny(new LinkedHashMap<>());
		children  = new ArrayList<Operator>();
		dataObjects = new HashSet<>();
	}
	
	public PostgreSQLIslandOperator(PostgreSQLIslandOperator o, boolean addChild) throws Exception {
		
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
				
				PostgreSQLIslandOperator op = (PostgreSQLIslandOperator)s.duplicate(addChild);
				op.setParent(this);
				this.children.add(op);
				
			}
		}
	}

	protected void populateComplexOutItem(boolean first) {
		// populate complexOutItemFromProgeny
		for (Operator c : children){
			PostgreSQLIslandOperator child = (PostgreSQLIslandOperator)c; 
			if ((!first) && child.getComplexOutItemFromProgeny().isEmpty()) child.populateComplexOutItem(first);
			for (String s: child.getOutSchema().keySet()) {
				Expression e = child.getOutSchema().get(s).getSQLExpression();
				if (e == null) continue;
				while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
				if (e instanceof Column) continue;
				getComplexOutItemFromProgeny().put(s, e.toString().replaceAll("[*]", "\\[\\*\\]").replaceAll("[.]", "\\[\\.\\]").replaceAll("[(]", "\\[\\(\\]").replaceAll("[)]", "\\[\\)\\]"));
			}
			if (first || (!child.isPruned()
					|| (child instanceof PostgreSQLIslandJoin && ((PostgreSQLIslandJoin)child).getJoinID() == null)
					|| (child instanceof PostgreSQLIslandAggregate && ((PostgreSQLIslandAggregate)child).getAggregateID() == null))) getComplexOutItemFromProgeny().putAll(child.getComplexOutItemFromProgeny());
		}
	}
	
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

	public void setCTERoot(boolean b) {
		isCTERoot = b;
	}
	
	@Override
	public void setParent(Operator p) {
		parent = p;
	}
	
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
	
	public boolean isAnyProgenyPruned() {
		if (this.isPruned) return true;
		else if (children.isEmpty()) return false;
		else return ((PostgreSQLIslandOperator)children.get(0)).isAnyProgenyPruned();
	}
	
	public Map<String, DataObjectAttribute>  getOutSchema() {
		return outSchema;
	}
	
	@Override
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
			result.putAll(((PostgreSQLIslandOperator) o).getTableLocations(map));
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
		if (!isPruned) return null;
		return BigDAWGPruneToken + this.pruneID;
	}
	
	public boolean isSubTree() {
		return this.isSubTree;
	}
	
	public void setSubTree(boolean t) {
		if (this instanceof PostgreSQLIslandJoin) return;
		if (t && this.subTreeID == null) {
			subTreeCount += 1;
			this.subTreeID = subTreeCount;
		}
		isSubTree = t;
	}
	
	public String getSubTreeToken() throws Exception {
		if (!isSubTree && !(this instanceof PostgreSQLIslandJoin)) return null;
		if (this instanceof PostgreSQLIslandJoin) return ((PostgreSQLIslandJoin)this).getJoinToken(); 
		else if (this instanceof PostgreSQLIslandAggregate && ((PostgreSQLIslandAggregate)this).getAggregateID() != null) return ((PostgreSQLIslandAggregate)this).getAggregateToken();
		else return BigDAWGSubtreeToken + this.subTreeID;
	}
	
	public void setQueryRoot(boolean isRoot) {
		this.isQueryRoot = isRoot;
	}
	
	public boolean isQueryRoot() {
		return this.isQueryRoot;
	}
	
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
		
		if (this instanceof PostgreSQLIslandJoin && ((PostgreSQLIslandJoin)this).joinID != null) {
			replaceToken = ((PostgreSQLIslandJoin)this).getJoinToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (this instanceof PostgreSQLIslandAggregate && ((PostgreSQLIslandAggregate)this).getAggregateID() != null) {
			replaceToken = ((PostgreSQLIslandAggregate)this).getAggregateToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (this.children.size() > 0 ) {
			
			if (this instanceof PostgreSQLIslandScan) {
				if (((PostgreSQLIslandScan)this).getTableAlias() != null && !((PostgreSQLIslandScan)this).getTableAlias().isEmpty())
					aliasOrString.put(((PostgreSQLIslandScan)this).getTableAlias(), ((PostgreSQLIslandScan)this).table.getFullyQualifiedName());
				else 
					aliasOrString.put(((PostgreSQLIslandScan)this).getSrcTable(), ((PostgreSQLIslandScan)this).table.getFullyQualifiedName());
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
			if (((PostgreSQLIslandScan)this).getTableAlias() != null && !((PostgreSQLIslandScan)this).getTableAlias().isEmpty())
				aliasOrString.put(((PostgreSQLIslandScan)this).getTableAlias(), ((PostgreSQLIslandScan)this).table.getFullyQualifiedName());
			else 
				aliasOrString.put(((PostgreSQLIslandScan)this).getSrcTable(), ((PostgreSQLIslandScan)this).table.getFullyQualifiedName());
		}
		
		return aliasOrString;
	}
	
	public List<PostgreSQLIslandOperator> getDataObjects() {
		List<PostgreSQLIslandOperator> extraction = new ArrayList<>();
		
		if (isPruned) {
			extraction.add(this);
			return extraction;
		}
		
		if (!(this instanceof PostgreSQLIslandScan )) {
			for (Operator o : children) {
				extraction.addAll(((PostgreSQLIslandOperator) o).getDataObjects());
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
	
	
	public Integer getBlockerID() throws Exception {
		if (!isBlocking)
			throw new Exception("PostgreSQLIslandOperator Not blocking: "+this.toString());
		return blockerID;
	}
	
	@Override
	public Operator duplicate(boolean addChild) throws Exception {
		if (this instanceof PostgreSQLIslandJoin) {
			return new PostgreSQLIslandJoin(this, addChild);
		} else if (this instanceof PostgreSQLIslandSeqScan) {
			return new PostgreSQLIslandSeqScan(this, addChild);
		} else if (this instanceof PostgreSQLIslandCommonTableExpressionScan) {
			return new PostgreSQLIslandCommonTableExpressionScan(this, addChild);
		} else if (this instanceof PostgreSQLIslandSort) {
			return new PostgreSQLIslandSort(this, addChild);
		} else if (this instanceof PostgreSQLIslandAggregate) {
			return new PostgreSQLIslandAggregate(this, addChild);
		} else if (this instanceof PostgreSQLIslandLimit) {
			return new PostgreSQLIslandLimit(this, addChild);
		} else if (this instanceof PostgreSQLIslandDistinct) {
			return new PostgreSQLIslandDistinct(this, addChild);
		} else if (this instanceof PostgreSQLIslandMerge) {
			return new PostgreSQLIslandMerge (this, addChild);
		} else {
			throw new Exception("Unsupported PostgreSQLIslandOperator Copy: "+this.getClass().toString());
		}
	}
	
	public void updateObjectAliases() {
		
		setObjectAliases(new HashSet<String>());
		if (this instanceof PostgreSQLIslandScan && ((PostgreSQLIslandScan)this).getTableAlias() != null) {
			getObjectAliases().add(((PostgreSQLIslandScan)this).getTableAlias());
		} else if (this instanceof PostgreSQLIslandJoin) {
			
			((PostgreSQLIslandOperator)children.get(1)).updateObjectAliases();
			getObjectAliases().addAll(((PostgreSQLIslandOperator)children.get(1)).getObjectAliases());
		} 

		if (children.size() != 0) {
			((PostgreSQLIslandOperator)children.get(0)).updateObjectAliases();
			getObjectAliases().addAll(((PostgreSQLIslandOperator)children.get(0)).getObjectAliases());
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
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		throw new Exception("Unimplemented: "+this.getClass().toString());
	}
	
	// half will be overriden
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		return children.get(0).getObjectToExpressionMappingForSignature();
	}
	
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws Exception {
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
	
	public Expression resolveAggregatesInFilter(String e, boolean goParent, PostgreSQLIslandOperator lastHopOp, Set<String> names, StringBuilder sb) throws Exception {
		
		if (goParent && parent != null) 
			return ((PostgreSQLIslandOperator)parent).resolveAggregatesInFilter(e, goParent, this, names, sb);
		else if (!goParent) {
			Expression exp = null;
			for (Operator o : children) {
				if (o == lastHopOp) break;
				if ((exp = ((PostgreSQLIslandOperator)o).resolveAggregatesInFilter(e, goParent, this, names, sb)) != null) return exp;
			}
		} 
		
		return null;
	} 
	
	public void seekScanAndProcessAggregateInFilter() throws Exception {
		for (Operator o : children) 
			((PostgreSQLIslandOperator)o).seekScanAndProcessAggregateInFilter();
	}
	
	public Map<String, Expression> getChildrenPredicates() throws Exception {
		return ((PostgreSQLIslandOperator) this.getChildren().get(0)).getChildrenPredicates();
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

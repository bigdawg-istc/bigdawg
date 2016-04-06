package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jcp.xml.dsig.internal.dom.Utils;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.plan.extract.CommonOutItem;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import istc.bigdawg.utils.sqlutil.SQLUtilities;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.SelectUtils;


// TODO: expressions on aggregates - e.g., COUNT(*) / COUNT(v > 5)

public class Aggregate extends Operator {

	// can address complex expressions by adding a step after aggregate
	// create a list of aggregations to perform
	
	public enum AggregateType { MIN, MAX, COUNT, COUNT_DISTINCT, AVG, SUM, WIDTH_BUCKET, DATE_PART};
	private List<SQLAttribute> groupBy;
	private List<String> aggregateExpressions; // e.g., COUNT(SOMETHING)
	private List<AggregateType>  aggregates; 
	private List<String> aggregateAliases; 
	private List<Function> parsedAggregates;
	private List<Expression> parsedGroupBys;
	private String aggregateFilter = null; // HAVING clause
	
	private static int maxAggregateID = 0;
	private static final String aggergateNamePrefix = "BIGDAWGAGGREGATE_";
	protected Integer aggregateID = null;
	
	
	// TODO: write ObliVM aggregate as a for loop over values, 
	// maintain state once per aggregate added
	// apply any expressions down the line in the final selection
	
	Aggregate(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		aggregateAliases = new ArrayList<String>(); 
		groupBy = new ArrayList<SQLAttribute>();
	
		parsedAggregates = new ArrayList<Function>();
		parsedGroupBys = new ArrayList<>();
		aggregateFilter = parameters.get("Filter");
		if(aggregateFilter != null) {
			aggregateFilter = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(aggregateFilter);
			Expression e = CCJSqlParserUtil.parseCondExpression(Utils.parseIdFromSameDocumentURI(aggregateFilter));
			SQLExpressionUtils.removeExcessiveParentheses(e);
			aggregateFilter = e.toString(); // HAVING clause
			
		}
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);

			
			SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
			SQLAttribute attr = out.getAttribute();
			String attrName = attr.getName();
			
			
			outSchema.put(attrName, attr);
			
			
			// e.g., sum(y) / count(x)
			if(out.hasAggregate()) {
				List<Function> parsedAggregates = out.getAggregates();
				
				for(int j = 0; j < parsedAggregates.size(); ++j) {
					processFunction(parsedAggregates.get(j), attrName);
				}
				
			}
			else {
				groupBy.add(attr);
				
			}
			
		}
		
		
		if (parameters.get("Group-Key") != null) {
			
			// pick out the outitems that are not columns
			Map<String, String> outExps = new HashMap<>();
			for (String s : outSchema.keySet()) {
				if (!s.equals(outSchema.get(s).getExpressionString()))
					outExps.put(s, outSchema.get(s).getExpressionString());
			}
			
			
			List<String> groupBysFromXML = Arrays.asList(SQLExpressionUtils
					.removeExpressionDataTypeArtifactAndConvertLike(parameters.get("Group-Key")).split("\n")); 
	
			
			for (String s : groupBysFromXML) {
				s = s.trim();
				if (s.isEmpty()) continue;
				Expression e = CCJSqlParserUtil.parseExpression(s);
				SQLExpressionUtils.removeExcessiveParentheses(e);
				
				while (e instanceof Parenthesis) e = ((Parenthesis) e).getExpression();
				
				String estr = e.toString();
				if (!(e instanceof Column))// && outExps.containsValue(estr))
					for (String str : outExps.keySet())
						if (outExps.get(str).contains(estr)) {
							// get it's children to stand-out as a subselect
							Set<String> names = new HashSet<>();
							StringBuilder sb = new StringBuilder();
							e = children.get(0).resolveAggregatesInFilter(estr, false, this, names, sb);
							
							if (e == null) {
								// then we look for the weird expression from outItem
								e = new Column(str);
							}
								
							break;
						}
							
//				if (e instanceof Column) System.out.printf("---->> e class: %s, %s, %s \n",e.getClass().getSimpleName(), ((Column) e).getColumnName(), ((Column) e).getTable());
				parsedGroupBys.add(e);
			}
//			System.out.println("parsedGroupBys: "+parsedGroupBys+"\n");
		}

	}
	
	public Aggregate(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Aggregate a = (Aggregate) o;
		
		this.aggregateID = a.aggregateID;
		this.groupBy = new ArrayList<SQLAttribute>();
		this.aggregateExpressions = new ArrayList<String>(); // e.g., COUNT(SOMETHING)
		this.aggregates = new ArrayList<AggregateType>(); 
		this.aggregateAliases = new ArrayList<String> (); 
		this.parsedAggregates = new ArrayList<Function>();
		this.parsedGroupBys = new ArrayList<Expression>();
		if (a.aggregateFilter != null)
			this.aggregateFilter = new String (a.aggregateFilter); // HAVING clause
		
		for (SQLAttribute att : a.groupBy)
			this.groupBy.add(new SQLAttribute(att));
		for (String ae : a.aggregateExpressions)
			this.aggregateExpressions.add(new String(ae));
		for (AggregateType at : a.aggregates)
			this.aggregates.add(at);
		for (String aa : a.aggregateAliases)
			this.aggregateAliases.add(new String (aa));
		for (Function pa : a.parsedAggregates) {
			Function f = new Function();
			f.setAllColumns(pa.isAllColumns());
			f.setAttribute(new String(pa.getAttribute()));
			f.setDistinct(pa.isDistinct());
			f.setEscaped(pa.isEscaped());
			f.setKeep(pa.getKeep());
			f.setName(new String(pa.getName()));
			f.setParameters(pa.getParameters());
			this.parsedAggregates.add(f);
		}
		for (Expression e : a.parsedGroupBys)
			this.parsedGroupBys.add(e);
		
	}
	
	
	// for AFL
	Aggregate(Map<String, String> parameters, SciDBArray output, Operator child) throws Exception  {
		super(parameters, output, child);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		aggregateAliases = new ArrayList<String>(); 
		groupBy = new ArrayList<SQLAttribute>();
	
		parsedAggregates = new ArrayList<Function>();
//		parsedGroupBys = supplement.getGroupBy();
		aggregateFilter = parameters.get("Filter");
//		if(aggregateFilter != null) {
//			aggregateFilter = Utils.parseIdFromSameDocumentURI(aggregateFilter); // HAVING clause
//		}
		
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for (String expr : output.getAttributes().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getAttributes().get(expr), true, null); // TODO CHECK THIS TODO
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getName();
			
			outSchema.put(attrName, attr);
			
			
			// e.g., sum(y) / count(x)
//			if(out.hasAggregate()) {
//				List<Function> parsedAggregates = out.getAggregates();
//				for(int j = 0; j < parsedAggregates.size(); ++j) {
//					processFunction(parsedAggregates.get(j), attrName);
//				}
//				
//				
//			}
//			else {
////				groupBy.add(attr);
//			}
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, "Dimension", true, null);
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		

	}
	
	
	void processFunction(Function f, String alias) throws Exception  {
		switch(f.getName()) {
			case "min":
				aggregates.add(AggregateType.MIN);
				break;
			case "max":
				aggregates.add(AggregateType.MAX);
				break;
			case "avg":
				aggregates.add(AggregateType.AVG);
				break;
			case "sum":
				aggregates.add(AggregateType.SUM);
				break;
			case "count":
				if(f.isDistinct())  {
						aggregates.add(AggregateType.COUNT_DISTINCT); }
				else {
					aggregates.add(AggregateType.COUNT); }
				break;
			case "width_bucket":
				aggregates.add(AggregateType.WIDTH_BUCKET);
				break;
			case "date_part":
				aggregates.add(AggregateType.DATE_PART);
				break;
			default:
				throw new Exception("Unknown aggregate type " + f.getName());
		}

		if(f.getParameters() != null) {
			String parameter = f.getParameters().toString();
			aggregateExpressions.add(parameter);
			parameter = SQLUtilities.removeOuterParens(parameter);
			// check for secure coordination
//			SQLAttribute attr = children.get(0).outSchema.get(parameter);
			
//			if(attr != null) {
//				updateSecurityPolicy(attr);
//			}
		}
		else {
			aggregateExpressions.add("");
		}
		
		aggregateAliases.add(alias);
			
	}
	
	public Aggregate() {
		isBlocking = true;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		
		
	}

	
	public void addAggregate(AggregateType a, String aFilter) {
		aggregates.add(a);
		aggregateExpressions.add(aFilter);
	}
	
	public String getAggregateToken() {
		if (aggregateID == null)
			return null;
		else
			return aggergateNamePrefix + aggregateID;
	}
	
	public void setSingledOutAggregate() {
		if (aggregateID == null) {
			maxAggregateID ++;
			aggregateID = maxAggregateID;
		}
	}
	
	@Override
	public Select generateSQLStringDestOnly(Select dstStatement, Boolean stopAtJoin, Set<String> allowedScans) throws Exception {

		Select originalDST = dstStatement;
		
//		String joinToken = null;
//		Operator o = this.getChildren().get(0);
//		while (!o.getChildren().isEmpty() && !(o instanceof Join)) o = o.getChildren().get(0);
//		if (o instanceof Join && stopAtJoin != null && stopAtJoin == true) joinToken = ((Join)o).getJoinToken();
//
//		
//		if (isPruned) {
//			Table t = new Table();
//			t.setName(this.getPruneToken());
//			dstStatement = SelectUtils.buildSelectFromTable(t);
//			
//			return dstStatement;
//		}
		
		
		if (aggregateID == null) dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, stopAtJoin, allowedScans);
		else dstStatement = children.get(0).generateSQLStringDestOnly(null, stopAtJoin, allowedScans);
				
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();

		if (ps.getSelectItems().get(0) instanceof AllColumns)
			ps.getSelectItems().remove(0);
		
		for (String alias: outSchema.keySet()) {
			
			Expression e = CCJSqlParserUtil.parseExpression(outSchema.get(alias).getSQLExpression().toString());
			SelectItem s = new SelectExpressionItem(e);
			
			if (!(e instanceof Column)) {
//				if (joinToken != null) SQLExpressionUtils.renameAttributes(e, this.getDataObjectAliasesOrNames().keySet(), joinToken);
				((SelectExpressionItem)s).setAlias(new Alias(alias));
			}
			
			ps.addSelectItems(s);
		}
		
		updateGroupByElements(stopAtJoin);
		ps.setGroupByColumnReferences(parsedGroupBys);
		if (aggregateFilter != null) {
			Expression e = CCJSqlParserUtil.parseCondExpression(aggregateFilter);
//			if (joinToken != null) SQLExpressionUtils.renameAttributes(e, this.getDataObjectAliasesOrNames().keySet(), joinToken);
			ps.setHaving(e);
		}
		
		
		if (aggregateID == null) return dstStatement;
		if (originalDST == null) {
			
			SubSelect ss = makeNewSubSelectUpdateDST(dstStatement);
			originalDST = SelectUtils.buildSelectFromTable(new Table()); // immediately replaced
			((PlainSelect)originalDST.getSelectBody()).setFromItem(ss);
			
//			System.out.printf("\n\n\nAggregate:\n\nonce: %s\n\n\n", originalDST.toString());
			
			return originalDST;
		}
		
		SubSelect ss = makeNewSubSelectUpdateDST(dstStatement);
		net.sf.jsqlparser.statement.select.Join insert = new net.sf.jsqlparser.statement.select.Join();
		insert.setRightItem(ss);
		insert.setSimple(true);
		
		PlainSelect pselect = (PlainSelect)originalDST.getSelectBody();
		
		System.out.printf("\n\n\nAggregate:\n\nbefore: %s\n\n", originalDST.toString());
		
		if (pselect.getJoins() != null) {
			boolean isFound = false;
			
			Map<String, String> aliasMapping = this.getDataObjectAliasesOrNames();
			
			for (int pos = 0; pos < pselect.getJoins().size(); pos++) { 
				FromItem r = pselect.getJoins().get(pos).getRightItem();
				if (!(r instanceof Table)) continue;
				
				Table t = (Table)r;
				if (t.getName().equals(this.getAggregateToken())) {
					pselect.getJoins().remove(pos);
					isFound = true;
				} else if (t.getAlias() != null && aliasMapping.containsKey(t.getAlias().getName()) && 
						aliasMapping.get(t.getAlias().getName()).equals(t.getName())) {
					pselect.getJoins().remove(pos);
					isFound = true;
				}
			}
			if (!isFound) {
				for (int pos = 0; pos < pselect.getJoins().size(); pos++) { 
					if (pselect.getJoins().get(pos).isSimple()) {
						pselect.getJoins().add(pos, insert);
						break;
					}
				}
			} else {
				pselect.getJoins().add(insert);
			}
		}
		
		System.out.printf("after: %s\n\n\n", originalDST.toString());
		
		return originalDST;
	}
	
	private SubSelect makeNewSubSelectUpdateDST(Select dstStatement) {
		SubSelect ss = new SubSelect();
		ss.setAlias(new Alias(this.getAggregateToken()));
		ss.setSelectBody(dstStatement.getSelectBody());
//		if (dstStatement.getWithItemsList() != null) ss.setWithItemsList(dstStatement.getWithItemsList());
		return ss;
	}
	
	public void updateGroupByElements(Boolean stopAtJoin) throws Exception {
		
		List<Operator> treeWalker;
		if (parsedGroupBys == null)
			return;
		for (Expression gb : parsedGroupBys) {
			
			treeWalker = children;
			boolean found = false;
			
			while (treeWalker.size() > 0 && (!found)) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						
						Column c = (Column)gb;
						
						if (o.getOutSchema().containsKey(c.getFullyQualifiedName())) {
							c.setTable(new Table(o.getPruneToken()));
							found = true;
							break;
						}
					} else if (o instanceof Join && (stopAtJoin != null && stopAtJoin == true )) {
						
						Column c = (Column)gb;
						
						if (o.getOutSchema().containsKey(c.getFullyQualifiedName())) {
							c.setTable(new Table(((Join)o).getJoinToken()));
							found = true;
							break;
						}
						
						
					} else {
						nextGeneration.addAll(o.children);
					}
				}
				
				treeWalker = nextGeneration;
			}
			
			
		}
	}
	
	@Override
	public String toString() {
		return "Aggregating on " + aggregateExpressions.toString() + " group by " + groupBy + " types " + aggregates.toString();
	}
	
	@Override
	public String generateAFLString(int recursionLevel) throws Exception {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("Aggregate(");
		sb.append(children.get(0).generateAFLString(recursionLevel+1));
		
		// TODO make sure the GroupBy are marked as hidden, otherwise do a redimension
		
//		System.out.println("outSchema:: "+outSchema);
//		System.out.println("AFL schema:: "+this.generateAFLCreateArrayStatementLocally("AAA"));
		
		for(int i = 0; i < aggregates.size(); ++i) {
			sb.append(", ").append(aggregates.get(i)).append(aggregateExpressions.get(i));
			if (aggregateAliases.get(i) != null)
				sb.append(" AS ").append(aggregateAliases.get(i));
			
		}
		updateGroupByElements(false);
		
		if(groupBy.size() > 0) {
			for(int i = 0; i < groupBy.size(); ++i) {
				sb.append(", ").append(groupBy.get(i).getName());
			}
		}

		sb.append(')');
		
		return sb.toString();
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else {
			StringBuilder sb = new StringBuilder();
			sb.append("{aggregate").append(children.get(0).getTreeRepresentation(false));
			
//			for (String alias: outSchema.keySet()) {
//				Expression e = outSchema.get(alias).getSQLExpression();
//				SQLExpressionUtils.removeExcessiveParentheses(e);
//				if (e instanceof Column) continue;
//				sb.append(SQLExpressionUtils.parseCondForTree(e));
//			}

			sb.append('}');
			return sb.toString();
		}
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		
		Operator parent = this;
		while (!parent.isBlocking && parent.parent != null ) parent = parent.parent;
		Map<String, String> aliasMapping = parent.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = children.get(0).getObjectToExpressionMappingForSignature();
		
//		// outItem
//		for (String s : outSchema.keySet()) {
//			Expression e = outSchema.get(s).getSQLExpression();
//			if (!(e instanceof Column || e instanceof AllColumns)) {
//				addToOut(e, out, aliasMapping);
//			};
//		}
		
		
		// having
		Expression e;
		if (aggregateFilter != null) {
			e = CCJSqlParserUtil.parseCondExpression(aggregateFilter);
			if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e)) {
				addToOut(e, out, aliasMapping);
			}
		} 
		
		System.out.printf("-----> aggregate getObjectToExpressionMappingForSignature: \n- %s; \n- %s; \n- %s",
				children.get(0).getObjectToExpressionMappingForSignature(),
				aliasMapping,
				out);
		
		return out;
	}

	@Override
	public Expression resolveAggregatesInFilter(String e, boolean goParent, Operator lastHopOp, Set<String> names, StringBuilder sb) throws Exception {
		
		for (String s: outSchema.keySet()) {
			Expression exp = outSchema.get(s).getSQLExpression();
			while (exp instanceof Parenthesis)
				exp = ((Parenthesis)exp).getExpression();
			if (e.equalsIgnoreCase(exp.toString())) {
				setSingledOutAggregate();
				
				Map<String, String> namesAndAliases = this.getDataObjectAliasesOrNames();
				names.addAll(namesAndAliases.keySet());
				
				sb.append(this.getAggregateToken());
				return new Column(new Table(getAggregateToken()), s);
			}
		}

		return super.resolveAggregatesInFilter(e, goParent, this, names, sb);
	}
	
	@Override
	protected Map<String, Expression> getChildrenIndexConds() throws Exception {
		Map<String, Expression> ret = this.getChildren().get(0).getChildrenIndexConds();
		ret.put(getAggregateToken(), null);
		return ret;
	}
};
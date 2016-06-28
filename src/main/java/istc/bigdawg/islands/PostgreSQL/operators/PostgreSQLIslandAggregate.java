package istc.bigdawg.islands.PostgreSQL.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jcp.xml.dsig.internal.dom.Utils;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLOutItemResolver;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.PostgreSQL.utils.SQLAttribute;
import istc.bigdawg.islands.PostgreSQL.utils.SQLExpressionUtils;
import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.Operator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


// TODO: expressions on aggregates - e.g., COUNT(*) / COUNT(v > 5)

public class PostgreSQLIslandAggregate extends PostgreSQLIslandOperator implements Aggregate {

	// can address complex expressions by adding a step after aggregate
	// create a list of aggregations to perform
	
//	public enum AggregateType { MIN, MAX, COUNT, COUNT_DISTINCT, AVG, SUM, WIDTH_BUCKET, DATE_PART};
//	private List<DataObjectAttribute> groupBy;
//	private List<String> aggregateExpressions; // e.g., COUNT(SOMETHING)
//	private List<AggregateType>  aggregates; 
	private List<String> aggregateAliases; 
//	private List<Function> parsedAggregates;
	private List<Expression> parsedGroupBys;
	private String aggregateFilter = null; // HAVING clause
	
	private static int maxAggregateID = 0;
//	private static final String aggergateNamePrefix = "BIGDAWGAGGREGATE_";
	private Integer aggregateID = null;
	
	
	// TODO: write ObliVM aggregate as a for loop over values, 
	// maintain state once per aggregate added
	// apply any expressions down the line in the final selection
	
	PostgreSQLIslandAggregate(Map<String, String> parameters, List<String> output, PostgreSQLIslandOperator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
//		aggregates = new ArrayList<AggregateType>();
//		aggregateExpressions = new ArrayList<String>(); 
		aggregateAliases = new ArrayList<String>(); 
//		groupBy = new ArrayList<DataObjectAttribute>();
	
//		parsedAggregates = new ArrayList<Function>();
		parsedGroupBys = new ArrayList<>();
		setAggregateFilter(parameters.get("Filter"));
		if(getAggregateFilter() != null) {
			setAggregateFilter(SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(getAggregateFilter()));
			Expression e = CCJSqlParserUtil.parseCondExpression(Utils.parseIdFromSameDocumentURI(getAggregateFilter()));
			SQLExpressionUtils.removeExcessiveParentheses(e);
			setAggregateFilter(e.toString()); // HAVING clause
			
		}
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);

			SQLOutItemResolver out = new SQLOutItemResolver(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
			SQLAttribute attr = out.getAttribute();
//			attr.setExpression(rewriteComplextOutItem(attr.getExpressionString()));
			String attrName = attr.getName();
			
			outSchema.put(attrName, attr);
			
//			// e.g., sum(y) / count(x)
//			if(out.hasAggregate()) {
//				List<Function> parsedAggregates = out.getAggregates();
//				
//				for(int j = 0; j < parsedAggregates.size(); ++j) {
//					processFunction(parsedAggregates.get(j), attrName);
//				}
//				
//			}
//			else {
//				groupBy.add(attr);
//				
//			}
			
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
				
				estr = rewriteComplextOutItem(estr);
				
//				if (!(e instanceof Column))// && outExps.containsValue(estr))
//					for (String str : outExps.keySet())
//						if (outExps.get(str).contains(estr)) {
//							// get it's children to stand-out as a subselect
//							Set<String> names = new HashSet<>();
//							StringBuilder sb = new StringBuilder();
//							e = children.get(0).resolveAggregatesInFilter(estr, false, this, names, sb);
//							
//							if (e == null) {
//								// then we look for the weird expression from outItem
//								e = new Column(str);
//							}
//								
//							break;
//						}
							
//				if (e instanceof Column) System.out.printf("---->> e class: %s, %s, %s \n",e.getClass().getSimpleName(), ((Column) e).getColumnName(), ((Column) e).getTable());
				parsedGroupBys.add(CCJSqlParserUtil.parseExpression(estr));
			}
//			System.out.println("parsedGroupBys: "+parsedGroupBys+"\n");
		}

	}
	
	public PostgreSQLIslandAggregate(PostgreSQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		
		this.blockerID = o.blockerID;
		
		PostgreSQLIslandAggregate a = (PostgreSQLIslandAggregate) o;
		
		this.setAggregateID(a.getAggregateID());
//		this.groupBy = new ArrayList<DataObjectAttribute>();
//		this.aggregateExpressions = new ArrayList<String>(); // e.g., COUNT(SOMETHING)
//		this.aggregates = new ArrayList<AggregateType>(); 
		this.aggregateAliases = new ArrayList<String> (); 
//		this.parsedAggregates = new ArrayList<Function>();
		this.parsedGroupBys = new ArrayList<Expression>();
		if (a.getAggregateFilter() != null)
			this.setAggregateFilter(new String (a.getAggregateFilter())); // HAVING clause
		
//		for (DataObjectAttribute att : a.groupBy)
//			this.groupBy.add(new DataObjectAttribute(att));
//		for (String ae : a.aggregateExpressions)
//			this.aggregateExpressions.add(new String(ae));
//		for (AggregateType at : a.aggregates)
//			this.aggregates.add(at);
		for (String aa : a.aggregateAliases)
			this.aggregateAliases.add(new String (aa));
//		for (Function pa : a.parsedAggregates) {
//			Function f = new Function();
//			f.setAllColumns(pa.isAllColumns());
//			f.setAttribute(new String(pa.getAttribute()));
//			f.setDistinct(pa.isDistinct());
//			f.setEscaped(pa.isEscaped());
//			f.setKeep(pa.getKeep());
//			f.setName(new String(pa.getName()));
//			f.setParameters(pa.getParameters());
//			this.parsedAggregates.add(f);
//		}
		for (Expression e : a.parsedGroupBys)
			this.parsedGroupBys.add(e);
		
	}
	
	
	
	
//	void processFunction(Function f, String alias) throws Exception  {
//		switch(f.getName()) {
//			case "min":
//				aggregates.add(AggregateType.MIN);
//				break;
//			case "max":
//				aggregates.add(AggregateType.MAX);
//				break;
//			case "avg":
//				aggregates.add(AggregateType.AVG);
//				break;
//			case "sum":
//				aggregates.add(AggregateType.SUM);
//				break;
//			case "count":
//				if(f.isDistinct())  {
//						aggregates.add(AggregateType.COUNT_DISTINCT); }
//				else {
//					aggregates.add(AggregateType.COUNT); }
//				break;
//			case "width_bucket":
//				aggregates.add(AggregateType.WIDTH_BUCKET);
//				break;
//			case "date_part":
//				aggregates.add(AggregateType.DATE_PART);
//				break;
//			default:
//				throw new Exception("Unknown aggregate type " + f.getName());
//		}
//
//		if(f.getParameters() != null) {
//			String parameter = f.getParameters().toString();
//			aggregateExpressions.add(parameter);
//			parameter = SQLUtilities.removeOuterParens(parameter);
//			// check for secure coordination
////			SQLAttribute attr = children.get(0).outSchema.get(parameter);
//			
////			if(attr != null) {
////				updateSecurityPolicy(attr);
////			}
//		}
//		else {
//			aggregateExpressions.add("");
//		}
//		
//		aggregateAliases.add(alias);
//			
//	}
	
	public PostgreSQLIslandAggregate() {
		isBlocking = true;
		
//		aggregates = new ArrayList<AggregateType>();
//		aggregateExpressions = new ArrayList<String>(); 
		
		
	}

	
//	public void addAggregate(AggregateType a, String aFilter) {
////		aggregates.add(a);
////		aggregateExpressions.add(aFilter);
//	}
	
	public String getAggregateToken() {
		if (getAggregateID() == null)
			return null;
		else
			return aggergateNamePrefix + getAggregateID();
	}
	
	public void setSingledOutAggregate() {
		if (getAggregateID() == null) {
			maxAggregateID ++;
			setAggregateID(maxAggregateID);
		}
	}
	
	public List<Expression> updateGroupByElements(Boolean stopAtJoin) throws Exception {
		
		List<Expression> ret = new ArrayList<>();
		
		List<Operator> treeWalker;
		if (parsedGroupBys == null)
			return null;
		for (Expression gb : parsedGroupBys) {
			
			treeWalker = children;
			boolean found = false;
			
			while (treeWalker.size() > 0 && (!found)) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						Column c = (Column)gb;
						
						if (((PostgreSQLIslandOperator)o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							ret.add(new Column(new Table(o.getPruneToken()), c.getColumnName()));
							found = true;
							break;
						}
					} else if (o instanceof PostgreSQLIslandJoin && stopAtJoin == true ) {
						
						Column c = (Column)gb;
						
						if (((PostgreSQLIslandOperator)o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							ret.add(new Column(new Table(((PostgreSQLIslandJoin)o).getJoinToken()), c.getColumnName()));
							found = true;
							break;
						}
					} else {
						nextGeneration.addAll(o.getChildren());
					}
				}
				if (found) continue; 
				treeWalker = nextGeneration;
			}
			
			
		}
		if (ret.isEmpty()) {
			for (Expression gb : parsedGroupBys) {
				ret.add(CCJSqlParserUtil.parseExpression(gb.toString()));
			}
		}
		return ret;
	}
	
//	@Override
//	public String toString() {
//		return "Aggregating on " + aggregateExpressions.toString() + " group by " + groupBy + " types " ;//+ aggregates.toString();
//	}
	
//	@Override
//	public String generateAFLString(int recursionLevel) throws Exception {
//		
//		StringBuilder sb = new StringBuilder();
//		
//		sb.append("Aggregate(");
//		sb.append(children.get(0).generateAFLString(recursionLevel+1));
//		
//		// TODO make sure the GroupBy are marked as hidden, otherwise do a redimension
//		
//		
//		for (String s : outSchema.keySet()) {
//			if (outSchema.get(s).isHidden()) continue;
//			sb.append(", ").append(outSchema.get(s).getExpressionString());
//			if (!outSchema.get(s).getName().contains("(")) sb.append(" AS ").append(outSchema.get(s).getName());
//		}
//		
//		List<Expression> updatedGroupBy = updateGroupByElements(false);
//		
//		if(updatedGroupBy.size() > 0) {
//			for(Expression e : updatedGroupBy) {
//				sb.append(", ").append(e);
//			}
//		}
//
//		sb.append(')');
//		
//		return sb.toString();
//	}
	
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
		while (!parent.isBlocking() && parent.getParent() != null ) parent = parent.getParent();
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
		if (getAggregateFilter() != null) {
			e = CCJSqlParserUtil.parseCondExpression(getAggregateFilter());
			if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e)) {
				addToOut(e, out, aliasMapping);
			}
		} 
		
//		System.out.printf("-----> aggregate getObjectToExpressionMappingForSignature: \n- %s; \n- %s; \n- %s",
//				children.get(0).getObjectToExpressionMappingForSignature(),
//				aliasMapping,
//				out);
		
		return out;
	}
	
	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}

	@Override
	public Expression resolveAggregatesInFilter(String e, boolean goParent, PostgreSQLIslandOperator lastHopOp, Set<String> names, StringBuilder sb) throws Exception {
		
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
	public Map<String, Expression> getChildrenPredicates() throws Exception {
		Map<String, Expression> ret = ((PostgreSQLIslandOperator) this.getChildren().get(0)).getChildrenPredicates();
		ret.put(getAggregateToken(), null);
		return ret;
	}

	public Integer getAggregateID() {
		return aggregateID;
	}

	public void setAggregateID(Integer aggregateID) {
		this.aggregateID = aggregateID;
	}

	public String getAggregateFilter() {
		return aggregateFilter;
	}

	public void setAggregateFilter(String aggregateFilter) {
		this.aggregateFilter = aggregateFilter;
	}
};
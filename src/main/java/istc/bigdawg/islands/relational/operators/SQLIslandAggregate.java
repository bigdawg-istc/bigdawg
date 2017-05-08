package istc.bigdawg.islands.relational.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jcp.xml.dsig.internal.dom.Utils;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.SQLOutItemResolver;
import istc.bigdawg.islands.relational.SQLTableExpression;
import istc.bigdawg.islands.relational.utils.SQLAttribute;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


// TODO: expressions on aggregates - e.g., COUNT(*) / COUNT(v > 5)

public class SQLIslandAggregate extends SQLIslandOperator implements Aggregate {

	// can address complex expressions by adding a step after aggregate
	// create a list of aggregations to perform
	
	private List<String> aggregateAliases; 
	private List<Expression> parsedGroupBys;
	private String aggregateFilter = null; // HAVING clause
	
	private static int maxAggregateID = 0;
	private static final String BigDAWGSQLAggregatePrefix = "BIGDAWGSQLAGGREGATE_";
	private Integer aggregateID = null;
	
	SQLIslandAggregate(Map<String, String> parameters, List<String> output, SQLIslandOperator child, SQLTableExpression supplement) throws QueryParsingException, JSQLParserException  {
		super(parameters, output, child, supplement);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		aggregateAliases = new ArrayList<String>(); 
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
			String attrName = attr.getName();
			
			outSchema.put(attrName, attr);
			
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
				
				parsedGroupBys.add(CCJSqlParserUtil.parseExpression(estr));
			}
		}

	}
	
	public SQLIslandAggregate(SQLIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		
		this.blockerID = o.blockerID;
		
		SQLIslandAggregate a = (SQLIslandAggregate) o;
		
		this.setAggregateID(a.getAggregateID());
		this.aggregateAliases = new ArrayList<String> (); 
		this.parsedGroupBys = new ArrayList<Expression>();
		if (a.getAggregateFilter() != null)
			this.setAggregateFilter(new String (a.getAggregateFilter())); // HAVING clause
		
		for (String aa : a.aggregateAliases)
			this.aggregateAliases.add(new String (aa));
		for (Expression e : a.parsedGroupBys)
			this.parsedGroupBys.add(e);
		
	}
	
	
	
	
	public SQLIslandAggregate() {
		isBlocking = true;
	}

	
	public String getAggregateToken() {
		if (getAggregateID() == null)
			return null;
		else
			return BigDAWGSQLAggregatePrefix + getAggregateID();
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
						
						if (((SQLIslandOperator)o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							ret.add(new Column(new Table(o.getPruneToken()), c.getColumnName()));
							found = true;
							break;
						}
					} else if (o instanceof SQLIslandJoin && stopAtJoin == true ) {
						
						Column c = (Column)gb;
						
						if (((SQLIslandOperator)o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							ret.add(new Column(new Table(((SQLIslandJoin)o).getJoinToken()), c.getColumnName()));
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
	
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException {
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else {
			StringBuilder sb = new StringBuilder();
			sb.append("{aggregate").append(children.get(0).getTreeRepresentation(false));
			
			sb.append('}');
			return sb.toString();
		}
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException {
		
		Operator parent = this;
		while (!parent.isBlocking() && parent.getParent() != null ) parent = parent.getParent();
		Map<String, String> aliasMapping = parent.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = children.get(0).getObjectToExpressionMappingForSignature();
		
		// having
		Expression e;
		if (getAggregateFilter() != null) {
			try {
				e = CCJSqlParserUtil.parseCondExpression(getAggregateFilter());
			} catch (JSQLParserException ex) {
				throw new IslandException(ex.getMessage(), ex);
			}
			if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e)) {
				addToOut(e, out, aliasMapping);
			}
		} 
		
		return out;
	}
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}

	@Override
	public Expression resolveAggregatesInFilter(String e, boolean goParent, SQLIslandOperator lastHopOp, Set<String> names, StringBuilder sb) throws IslandException {
		
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
		Map<String, Expression> ret = ((SQLIslandOperator) this.getChildren().get(0)).getChildrenPredicates();
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
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(Aggregate ");
		sb.append(this.parsedGroupBys).append(' ');
		sb.append(this.getOutSchema()).append(' ');
		if (children.get(0) != null) sb.append(' ').append(children.get(0));
		sb.append(')');
		return sb.toString();
	}
};
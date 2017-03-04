package istc.bigdawg.islands.SciDB.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.SciDB.SciDBAttributeOrDimension;
import istc.bigdawg.islands.SciDB.SciDBParsedArray;
import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


public class SciDBIslandAggregate extends SciDBIslandOperator implements Aggregate {

	private List<String> aggregateAliases; 
	private List<Expression> parsedGroupBys;
	private String aggregateFilter = null; // HAVING clause
	
	protected static final String BigDAWGSciDBAggregatePrefix = "BIGDAWGSCIDBAGGREGATE_";
	private static int maxAggregateID = 0;
	private Integer aggregateID = null;
	
	
	
	public SciDBIslandAggregate(SciDBIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		SciDBIslandAggregate a = (SciDBIslandAggregate) o;
		
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
	
	
	// for AFL
	SciDBIslandAggregate(Map<String, String> parameters, SciDBParsedArray output, Operator child) throws JSQLParserException {
		super(parameters, output, child);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		List<String> aggregateExpressions = Arrays.asList(parameters.get("Aggregate-Functions").split(", ")); 
		aggregateAliases = new ArrayList<String>();
		parsedGroupBys = new ArrayList<>();
		setAggregateFilter(parameters.get("Filter"));
		
		if (parameters.get("Aggregate-Dimensions") != null) {
			List<String> aggDims = Arrays.asList(parameters.get("Aggregate-Dimensions").split("[|][|][|]"));
			for (String s : aggDims) parsedGroupBys.add(CCJSqlParserUtil.parseExpression(s));
		} 
		
		Map<String, String> aggFuns = new HashMap<>();
		for (String s : aggregateExpressions) {
			String alias = null;
			
			try {
				Expression f = CCJSqlParserUtil.parseExpression(s);
				
				if (f instanceof Function) {
					List<String> exprAndAlias = Arrays.asList(s.split(" AS "));
					if (exprAndAlias.size() > 1) alias = exprAndAlias.get(1);
					else alias = ((Function)f).getParameters().getExpressions().get(0).toString()+"_"+((Function)f).getName();
					
				} else if (f instanceof Column) {
					alias = ((Column)f).getColumnName();
				}
				aggFuns.put(alias.toLowerCase(), f.toString());
			} catch (Exception e) {
				String[] segs = s.split("[-\\(\\)\\.,\\*\\/\\+\\s]+");
				aggFuns.put((segs[1]+"_"+segs[0]).toLowerCase(), s);
			}
			
			
		}
		
		System.out.printf("aggFun: %s\n", aggFuns);
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for (String expr : output.getAttributes().keySet()) {

			SciDBAttributeOrDimension attr = new SciDBAttributeOrDimension();
			
			attr.setName(expr);
			attr.setTypeString(output.getAttributes().get(expr));
			attr.setHidden(false);
			
			String exprLowerCase = expr.toLowerCase();
			
			if (aggFuns.get(exprLowerCase) != null) attr.setExpression(aggFuns.get(exprLowerCase));
			else attr.setExpression(expr);
			
			outSchema.put(attr.getName(), attr);
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			SciDBAttributeOrDimension dim = new SciDBAttributeOrDimension();
			
			dim.setName(expr);
			dim.setTypeString(output.getDimensions().get(expr));
			dim.setHidden(true);
			
			Column e = (Column) CCJSqlParserUtil.parseExpression(expr);
			String arrayName = output.getDimensionMembership().get(expr);
			if (arrayName != null) {
				e.setTable(new Table(Arrays.asList(arrayName.split(", ")).get(0)));
			}
			
			dim.setExpression(e);
			
			String dimName = dim.getFullyQualifiedName();		
			outSchema.put(dimName, dim);
				
		}
		
	}
	
	
	
	public SciDBIslandAggregate() {
		isBlocking = true;
		
	}

	
	public String getAggregateToken() {
		if (getAggregateID() == null)
			return null;
		else
			return BigDAWGSciDBAggregatePrefix + getAggregateID();
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
						
						if (((SciDBIslandOperator) o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							ret.add(new Column(new Table(o.getPruneToken()), c.getColumnName()));
							found = true;
							break;
						}
					} else if (o instanceof SciDBIslandJoin && stopAtJoin == true ) {
						
						Column c = (Column)gb;
						
						if (((SciDBIslandOperator) o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							ret.add(new Column(new Table(((SciDBIslandJoin)o).getJoinToken()), c.getColumnName()));
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
	public String getTreeRepresentation(boolean isRoot) throws IslandException{
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else {
			StringBuilder sb = new StringBuilder();
			sb.append("{aggregate").append(children.get(0).getTreeRepresentation(false));

			sb.append('}');
			return sb.toString();
		}
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException{
		
		Operator parent = this;
		while (!parent.isBlocking() && parent.getParent() != null ) parent = parent.getParent();
		Map<String, String> aliasMapping = parent.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = children.get(0).getObjectToExpressionMappingForSignature();
		
		
		// having
		Expression e;
		try {
			if (getAggregateFilter() != null) {
				e = CCJSqlParserUtil.parseCondExpression(getAggregateFilter());
				if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e)) {
					addToOut(e, out, aliasMapping);
				}
			} 
		} catch (JSQLParserException ex) {
			throw new IslandException(ex.getMessage(), ex);
		}
		
		return out;
	}
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
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
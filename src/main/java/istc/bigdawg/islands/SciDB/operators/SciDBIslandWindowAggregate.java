package istc.bigdawg.islands.SciDB.operators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.CommonOutItemResolver;
import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.WindowAggregate;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class SciDBIslandWindowAggregate extends SciDBIslandOperator implements WindowAggregate {

	private List<Integer> dimensionBounds;
	private List<String> functions;
	
	// for AFL
	SciDBIslandWindowAggregate(Map<String, String> parameters, SciDBArray output, Operator child) throws JSQLParserException {
		super(parameters, output, child);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		dimensionBounds = Arrays.asList(parameters.get("Window-Dimension-Parameters").split(", ")).stream().map(Integer::parseInt).collect(Collectors.toList());
		functions = Arrays.asList(parameters.get("Window-Aggregate-Functions").split(", "));
		
		Map<String, String> aggFuns = new HashMap<>();
		for (String s : functions) {
			String alias = null;
			
			try {
				Expression f = CCJSqlParserUtil.parseExpression(s);
				
				if (f instanceof Function) {
					List<String> exprAndAlias = Arrays.asList(s.split(" AS "));
					if (exprAndAlias.size() > 1) alias = exprAndAlias.get(1);
					else if (((Function)f).isAllColumns()) alias = ((Function)f).getName();
					else alias = ((Function)f).getParameters().getExpressions().get(0).toString()+"_"+((Function)f).getName();
				} else if (f instanceof Column) {
					alias = ((Column)f).getColumnName();
				}
				aggFuns.put(alias, f.toString());
			} catch (Exception e) {
				
				e.printStackTrace();
				
				String[] segs = s.split("[-\\(\\)\\.,\\*\\/\\+\\s]+");
				aggFuns.put(segs[1]+"_"+segs[0], s);
			}
			
			
		}
		
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for (String expr : output.getAttributes().keySet()) {
			
			CommonOutItemResolver out = new CommonOutItemResolver(expr, output.getAttributes().get(expr), false, null); // TODO CHECK THIS TODO
			DataObjectAttribute attr = out.getAttribute();
			
			if (aggFuns.get(expr) != null) attr.setExpression(aggFuns.get(expr));
			else attr.setExpression(expr);
			
			String attrName = attr.getName();
			
			outSchema.put(attrName, attr);
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItemResolver out = new CommonOutItemResolver(expr, "Dimension", true, null);
			DataObjectAttribute dim = out.getAttribute();
			
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
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	public String toString() {
		return "(window (" + children + ") " 
				+ String.join(", ", dimensionBounds.stream().map(String::valueOf).collect(Collectors.toSet())) 
				+ String.join(", ", functions)
				+ ")";
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException {
		return "{window"+children.get(0).getTreeRepresentation(false)+"}";
	}
	
	public List<Integer> getDimensionBounds() {
		return dimensionBounds;
	}

	public void setDimensionBounds(List<Integer> dimensionBounds) {
		this.dimensionBounds = dimensionBounds;
	}

	public List<String> getFunctions() {
		return functions;
	}

	public void setFunctions(List<String> functions) {
		this.functions = functions;
	}

	
};
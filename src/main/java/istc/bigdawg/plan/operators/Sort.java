package istc.bigdawg.plan.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Sort extends Operator {

	private List<String> sortKeys;
	
	public enum SortOrder {ASC, DESC}; // ascending or descending?
	
	private SortOrder sortOrder;
	
	private List<OrderByElement> orderByElements;
	
	protected boolean isWinAgg = false; // is it part of a windowed aggregate or an ORDER BY clause?
	
	public Sort(Map<String, String> parameters, List<String> output,  List<String> keys, Operator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		isBlocking = true;
//		isSplittable = true;
		isLocal = false;
		

		// two order bys might exist in a supplement:
		// 1) within an OVER () clause for windowed aggregate
		// 2) as an ORDER BY clause
		// instantiate iterator to get the right one
		// iterate from first OVER --> ORDER BY

		sortKeys = keys;

		sortOrder = supplement.getSortOrder(sortKeys);
		
//		secureCoordination = children.get(0).secureCoordination;
		outSchema = new LinkedHashMap<String, SQLAttribute>(child.outSchema);
		
		
		// match with previous schema to get any aliases to propagate
		for(int i = 0; i < sortKeys.size(); ++i) {
			String a = supplement.getAlias(sortKeys.get(i));
			if(a != null) {
				sortKeys.set(i, a);
			}
			
			// if we sort on a protected or private key, then go to SMC
			// only simple expressions supported, no additional arithmetic ops
//			SQLAttribute attr = outSchema.get(sortKeys.get(i));
//			updateSecurityPolicy(attr);
		}
		
		orderByElements = supplement.getOrderByClause();
		

	
	}
	
	@Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {
		dstStatement = children.get(0).generatePlaintext(srcStatement, dstStatement);

		if(!isWinAgg) {
			PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
			ps.setOrderByElements(orderByElements);
		}
		
		return dstStatement;

	}
	
	@Override
	public List<SQLAttribute> getSliceKey()  throws JSQLParserException {
		
		if(isWinAgg) {
			assert(parent instanceof WindowAggregate);
			return parent.getSliceKey();
			}
		
		return null;
		
	}

	
	public String toString() {
		return "Sort operator on columns " + sortKeys.toString() + " with ordering " + sortOrder;
	}
	
	
	public String printPlan(int recursionLevel) {
		String planStr = "Sort(";
		planStr += children.get(0).printPlan(recursionLevel+1);
		planStr += ", " + sortKeys.toString() + "," + sortOrder + ")";
		return planStr;
		
	}
	
};
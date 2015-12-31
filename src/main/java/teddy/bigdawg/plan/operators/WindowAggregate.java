package teddy.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import teddy.bigdawg.extract.logical.SQLTableExpression;
import teddy.bigdawg.plan.SQLQueryPlan;
import teddy.bigdawg.plan.extract.SQLOutItem;
import teddy.bigdawg.schema.SQLAttribute;
import teddy.bigdawg.util.SQLExpressionUtils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;

public class WindowAggregate extends Operator {

	List<String> winaggs;
	protected List<ExpressionList> partitionBy;

	// order by is mostly ignored because psql plan 
	// rewrites this as a sort nested below the WindowAgg
	protected List<List<OrderByElement> > orderBy;
	
	List<AnalyticExpression> parsedAggregates;
	
	WindowAggregate(Map<String, String> parameters, List<String> output, Operator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		isBlocking = true;
//		isSplittable = true;
		isLocal = false;

		winaggs = new ArrayList<String>();
		
		partitionBy = new ArrayList<ExpressionList>();
		orderBy =  new ArrayList<List<OrderByElement> >();
		
		
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);
				
			SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement);
			SQLAttribute attr = out.getAttribute();
			String alias = attr.getName();
			
			outSchema.put(alias, attr);
			

			if(out.hasWindowedAggregates()) {

				parsedAggregates = out.getWindowedAggregates();
				List<AnalyticExpression> ae = out.getWindowedAggregates();
				for(int j = 0; j < ae.size(); ++j) {
					AnalyticExpression e = ae.get(j);

					winaggs.add(e.getName());
					
					assert(e.getName().equals("row_number()")); // others are not yet implemented
						
					partitionBy.add(e.getPartitionExpressionList());
					orderBy.add(e.getOrderByElements());
				}
			}
			
	
			
		}
		
			
		
		if(partitionBy.size() > 0) {
			// if this is (PARTITION BY x ORDER BY y) push down slice key to sort
			// want to slice as fine as possible to break up SMC groups
			if(child instanceof Sort && !orderBy.isEmpty()) {
				Sort c = (Sort) child;
				c.isWinAgg = true;
			}
		}
		
//		secureCoordination = children.get(0).secureCoordination;
		
		// for simple WindowAggregate (i.e., row_number) no attributes accessed
		//if order by something protected|private, then update policy
		

	}
	
	
	@Override
	public List<SQLAttribute> getSliceKey() throws JSQLParserException {
		List<SQLAttribute> sliceKey = new ArrayList<SQLAttribute>();
				
		if(partitionBy != null && partitionBy.size() > 0) {
			for(ExpressionList l : partitionBy) {
				for(Expression e : l.getExpressions()) {
					List<String> candidateKeys = SQLExpressionUtils.getAttributes(e.toString());
					for(String s : candidateKeys) {
						SQLAttribute a = outSchema.get(s); // src schema and out schema are the same
						if(a == null) {
							// iterate over outSchema and find match with fully qualified name
							for(String name : outSchema.keySet()) {
								String attrName = name.substring(name.indexOf(".")+1);
								if(attrName.equals(s))  {
									a = outSchema.get(name);
									break;
								}
							}
						}
						
						
//						if(a.getSecurityPolicy().equals(SQLAttribute.SecurityPolicy.Public)) {
							sliceKey.add(a);
//						}
					} // end candidate keys
					
				} // end expressions for a single windowed aggregate
			} // end iterator over all partition bys
		}
		
		return sliceKey;
	}
	
	
	
	@Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {
		dstStatement = children.get(0).generatePlaintext(srcStatement, dstStatement);
		// do nothing here until SELECT clause		
	
		return dstStatement;

	}
	
	
	public String toString() {
		return "WindowAgg over " + winaggs + " partition by " + partitionBy + " order by " + orderBy;
	}
	
	public String printPlan(int recursionLevel) {
		String planStr =  "WindowAgg(";
		planStr +=  children.get(0).printPlan(recursionLevel+1);
		planStr += winaggs + "," + partitionBy + "," + orderBy + ")";
		return planStr;
	}
};
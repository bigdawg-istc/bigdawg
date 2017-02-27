package istc.bigdawg.islands.relational.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.operators.WindowAggregate;
import istc.bigdawg.islands.relational.SQLOutItemResolver;
import istc.bigdawg.islands.relational.SQLTableExpression;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SQLIslandWindowAggregate extends SQLIslandOperator implements WindowAggregate {

	List<String> winaggs;
	protected List<ExpressionList> partitionBy;

	// order by is mostly ignored because psql plan 
	// rewrites this as a sort nested below the WindowAgg
	protected List<List<OrderByElement> > orderBy;
	
	List<AnalyticExpression> parsedAggregates;
	
	SQLIslandWindowAggregate(Map<String, String> parameters, List<String> output, SQLIslandOperator child, SQLTableExpression supplement)
		throws QueryParsingException, JSQLParserException {
		super(parameters, output, child, supplement);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		

		winaggs = new ArrayList<String>();
		
		partitionBy = new ArrayList<ExpressionList>();
		orderBy =  new ArrayList<List<OrderByElement> >();
		
		
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);
				
			SQLOutItemResolver out = new SQLOutItemResolver(expr, child.outSchema, supplement);
			DataObjectAttribute attr = out.getAttribute();
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
			if(child instanceof SQLIslandSort && !orderBy.isEmpty()) {
				SQLIslandSort c = (SQLIslandSort) child;
				c.setWinAgg(true);
			}
		}
		
	}
	
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	
	public String toString() {
		return "WindowAgg over " + winaggs + " partition by " + partitionBy + " order by " + orderBy;
	}
};
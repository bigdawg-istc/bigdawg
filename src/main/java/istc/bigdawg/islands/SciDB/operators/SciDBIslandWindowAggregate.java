package istc.bigdawg.islands.SciDB.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import istc.bigdawg.islands.CommonOutItemResolver;
import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.WindowAggregate;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SciDBIslandWindowAggregate extends SciDBIslandOperator implements WindowAggregate {

	List<String> winaggs;
	protected List<ExpressionList> partitionBy;

	// order by is mostly ignored because psql plan 
	// rewrites this as a sort nested below the WindowAgg
	protected List<List<OrderByElement> > orderBy;
	
	List<AnalyticExpression> parsedAggregates;
	
//	SciDBIslandWindowAggregate(Map<String, String> parameters, List<String> output, SciDBIslandOperator child, SQLTableExpression supplement) throws Exception  {
//		super(parameters, output, child, supplement);
//
//		isBlocking = true;
//		blockerCount++;
//		this.blockerID = blockerCount;
//		
//
//		winaggs = new ArrayList<String>();
//		
//		partitionBy = new ArrayList<ExpressionList>();
//		orderBy =  new ArrayList<List<OrderByElement> >();
//		
//		
//		for(int i = 0; i < output.size(); ++i) {
//			String expr = output.get(i);
//				
//			SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement);
//			DataObjectAttribute attr = out.getAttribute();
//			String alias = attr.getName();
//			
//			outSchema.put(alias, attr);
//			
//
//			if(out.hasWindowedAggregates()) {
//
//				parsedAggregates = out.getWindowedAggregates();
//				List<AnalyticExpression> ae = out.getWindowedAggregates();
//				for(int j = 0; j < ae.size(); ++j) {
//					AnalyticExpression e = ae.get(j);
//
//					winaggs.add(e.getName());
//					
//					assert(e.getName().equals("row_number()")); // others are not yet implemented
//						
//					partitionBy.add(e.getPartitionExpressionList());
//					orderBy.add(e.getOrderByElements());
//				}
//			}
//			
//	
//			
//		}
//		
//			
//		
//		if(partitionBy.size() > 0) {
//			// if this is (PARTITION BY x ORDER BY y) push down slice key to sort
//			// want to slice as fine as possible to break up SMC groups
//			if(child instanceof SciDBIslandSort && !orderBy.isEmpty()) {
//				SciDBIslandSort c = (SciDBIslandSort) child;
//				c.setWinAgg(true);
//			}
//		}
//		
////		secureCoordination = children.get(0).secureCoordination;
//		
//		// for simple WindowAggregate (i.e., row_number) no attributes accessed
//		//if order by something protected|private, then update policy
//		
//
//	}
	
	
	// for AFL
	SciDBIslandWindowAggregate(Map<String, String> parameters, SciDBArray output, Operator child) throws Exception  {
		super(parameters, output, child);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		winaggs = new ArrayList<String>();
		
		partitionBy = new ArrayList<ExpressionList>();
		orderBy =  new ArrayList<List<OrderByElement> >();
		
		
		for (String expr : output.getAttributes().keySet()) {
			CommonOutItemResolver out = new CommonOutItemResolver(expr, output.getAttributes().get(expr), false, null);
			DataObjectAttribute attr = out.getAttribute();
			String alias = attr.getName();
			
			outSchema.put(alias, attr);
			

//			if(out.hasWindowedAggregates()) {
//
//				parsedAggregates = out.getWindowedAggregates();
//				List<AnalyticExpression> ae = out.getWindowedAggregates();
//				for(int j = 0; j < ae.size(); ++j) {
//					AnalyticExpression e = ae.get(j);
//
//					winaggs.add(e.getName());
//					
//					assert(e.getName().equals("row_number()")); // others are not yet implemented
//						
//					partitionBy.add(e.getPartitionExpressionList());
//					orderBy.add(e.getOrderByElements());
//				}
//			}
			
	
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItemResolver out = new CommonOutItemResolver(expr, "Dimension", true, null);
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		
		if(partitionBy.size() > 0) {
			// if this is (PARTITION BY x ORDER BY y) push down slice key to sort
			// want to slice as fine as possible to break up SMC groups
			if(child instanceof SciDBIslandSort && !orderBy.isEmpty()) {
				SciDBIslandSort c = (SciDBIslandSort) child;
				c.setWinAgg(true);
			}
		}
		

	}
	
	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	
	public String toString() {
		return "WindowAgg over " + winaggs + " partition by " + partitionBy + " order by " + orderBy;
	}
	
//	@Override
//	public String generateAFLString(int recursionLevel) throws Exception{
//		String planStr =  "WindowAgg(";
//		planStr +=  children.get(0).generateAFLString(recursionLevel+1);
//		planStr += winaggs + "," + partitionBy + "," + orderBy + ")";
//		return planStr;
//	}
};
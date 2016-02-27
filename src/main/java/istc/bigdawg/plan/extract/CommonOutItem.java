package istc.bigdawg.plan.extract;

import java.util.List;
import java.util.Map;

import istc.bigdawg.schema.DataObjectAttribute;

public class CommonOutItem {

	// takes in the content of a <Item> field in EXPLAIN xml 
	// keeps track of fields referenced in expression for security level 
	// and any function / aggregates referenced
	
	// supports:
	// column references
	// math expression
	// aggregate 
	// windowed aggregate
	
	// TODO: expressions with multiple aggregates might not work
	
	protected List<String> commonAggregates;
	protected List<String> commonWindowedAggregates;
	protected String alias = null; // attr alias
	protected DataObjectAttribute outAttribute;
	
	
	public CommonOutItem(String alias, String type, boolean hidden, Map<String, DataObjectAttribute> srcSchema) throws Exception {
		
		outAttribute = new DataObjectAttribute();
		
		
//		aggregates = new ArrayList<Function>();
//		windowedAggregates = new ArrayList<AnalyticExpression>();

//		if(supplement != null) {
//			alias = supplement.getAlias(expr);
//		}
		
		this.alias = alias;
		
		outAttribute.setName(alias);
		outAttribute.setTypeString(type);
		outAttribute.setHidden(hidden);
		
		
//			public void visit(Column tableColumn) {
//			// simple aggregates
//			// windowed aggregate
//			public void visit(AnalyticExpression aexpr) {
//				super.visit(aexpr);
//				// grab aexpr from supplement
//				
//				AnalyticExpression fullExpression = supplement.getAnalyticExpression();
//				windowedAggregates.add(fullExpression);
//				assert(aexpr.getName() == "row_number"); // all others not yet implemented
//				setUpAggregateAllColumns(srcSchema, outAttribute);  // TODO: make this more fine grained, only derived from ORDER BY, PARTITION BY and possibly aggregate
//				outAttribute.setExpression(fullExpression); // replace predecessor
//				
//			}
		
	}
	
	public CommonOutItem() {
		
	}
	
//	// takes in alias src, determines if it has a match in src schema
//	// if so, it prefixes the column reference with the src table
//	String fullyQualify(String expr, Map<String, DataObjectAttribute> srcSchema) throws JSQLParserException {
//		
//		ExpressionDeParser deparser = new ExpressionDeParser() {
//		
//			
//			public void visit(Column tableColumn) {
//				
//				if(tableColumn.getTable().getName() == null) {
//					
//					for(String s : srcSchema.keySet()) {
//						final String[] names = s.split("\\.");
//
//
//						if(tableColumn.getColumnName().equalsIgnoreCase(names[1])) { // happens just once
//							final Table t = new Table(names[0]);
//							tableColumn.setTable(t);
//						}
//					}
//										
//				}
//				
//				super.visit(tableColumn);
//			}
//			
//		
//		}; // end expression parser
//		
//		  StringBuilder b = new StringBuilder();
//		  deparser.setBuffer(b);
//  		  Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
//		  parseExpression.accept(deparser);
//		  return b.toString();
//
//	}
		

	
	
	public DataObjectAttribute getAttribute() {
		return outAttribute;
	}
	
	public boolean hasAggregate() {
		return !commonAggregates.isEmpty();
	}
	
	public boolean hasWindowedAggregates() {
		return !commonWindowedAggregates.isEmpty();
	}
	
	public List<String> getCommonAggregates() {
		return commonAggregates;
	}
	
	public List<String> getCommonWindowedAggregates() {
		return commonWindowedAggregates;
	}
}

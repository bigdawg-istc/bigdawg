package istc.bigdawg.util;

import java.util.Map;

import istc.bigdawg.schema.SQLAttribute;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class SQLAggregateExpression {


	// iterate over src schema for * operators to determine max security level

	public static SQLAttribute getSQLAttribute(String expr, Map<String, SQLAttribute> srcSchema, String outName) throws JSQLParserException {

	
		final SQLAttribute attr = new SQLAttribute();
    	boolean isInit = false;
    	
    	Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
	
	
    	ExpressionDeParser deparser = new ExpressionDeParser() {

		public void visit(Column tableColumn) {
			String name = tableColumn.getColumnName();
			SQLAttribute lookup = srcSchema.get(name);
			if(isInit == false) {
//				attr.setSecurityPolicy(lookup.getSecurityPolicy());
				attr.setName(outName);
				attr.setType(lookup.getType());
			}
			else {
				// check to make sure it is the same type
				// if security level different, pick highest one
//				if(lookup.getSecurityPolicy().compareTo(attr.getSecurityPolicy()) > 0) {
//					attr.setSecurityPolicy(lookup.getSecurityPolicy());					
//				}
				
				assert(attr.getType().getDataType() == lookup.getType().getDataType());
			}
		}
		
		public void visit(Function function) { 
			
		}

        
		
	    };

	    StringBuilder b = new StringBuilder();
	    deparser.setBuffer(b);
	    parseExpression.accept(deparser);
	    
	    return attr;
	    
    }
}

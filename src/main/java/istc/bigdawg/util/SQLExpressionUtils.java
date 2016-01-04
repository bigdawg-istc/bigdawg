package istc.bigdawg.util;

import java.util.ArrayList;
import java.util.List;
import istc.bigdawg.extract.logical.SQLExpressionHandler;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

public class SQLExpressionUtils {

	public static List<Expression> getEqualityPredicates(String expr) throws JSQLParserException {

		final List<Expression> equalities = new ArrayList<Expression>();
		Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
				super.visitBinaryExpression(binaryExpression, operator);
				if(operator.equals("=")) {
					equalities.add(binaryExpression);
				}

			}
	        
	    };
	    StringBuilder b = new StringBuilder();
	    deparser.setBuffer(b);
	    parseExpression.accept(deparser);

		return equalities;
	}


	
	public static List<String> getAttributes(String expr) throws JSQLParserException {

		
		Expression parsedExpression = null;
		
		try {
			parsedExpression = CCJSqlParserUtil.parseExpression(expr);
		}
		catch(JSQLParserException j) {
			System.out.print("Throw: " + j.toString());
		}
		
		// try conditional expression
		// raise alert if it fails
		if(parsedExpression == null) {
			 parsedExpression = CCJSqlParserUtil.parseCondExpression(expr);	
		}
		
		
		
		return getAttributes(parsedExpression);
	}
	
	public static List<String> getAttributes(Expression expr) throws JSQLParserException {
		
		final List<String> attributes = new ArrayList<String>();

		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				super.visit(tableColumn);
				attributes.add(tableColumn.getFullyQualifiedName());
			}
			
		
	    };
	    
	    StringBuilder b = new StringBuilder();
	    deparser.setBuffer(b);
	    expr.accept(deparser);
	    
		return attributes;

	
	}


}

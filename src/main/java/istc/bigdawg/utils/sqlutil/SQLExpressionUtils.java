package istc.bigdawg.utils.sqlutil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLExpressionHandler;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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


	
//	public static List<String> getAttributes(String expr) throws JSQLParserException {
//
//		
//		Expression parsedExpression = null;
//		
//		try {
//			parsedExpression = CCJSqlParserUtil.parseExpression(expr);
//		}
//		catch(JSQLParserException j) {
//			// do nothing
//		}
//		
//		// try conditional expression
//		// raise alert if it fails
//		if(parsedExpression == null) {
//			 parsedExpression = CCJSqlParserUtil.parseCondExpression(expr);	
//		}
//		
//		
//		
//		return getAttributes(parsedExpression);
//	}

	
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
	
	
	
	public static void renameAttributes(Expression expr, Set<String> originalTableNameSet, String replacement) throws JSQLParserException {
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				if (originalTableNameSet == null || originalTableNameSet.contains(tableColumn.getTable().getName()))
					tableColumn.getTable().setName(replacement);
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {
		        parenthesis.getExpression().accept(this);
		    }
			
			@Override
			protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
		        binaryExpression.getLeftExpression().accept(this);
		        binaryExpression.getRightExpression().accept(this);
			}
			
			@Override
		    public void visit(ExpressionList expressionList) {
		        for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
		            Expression expression = iter.next();
		            expression.accept(this);
		        }
		    }
			
			@Override
			public void visit(Function function) {
				function.getParameters().accept(this);
			}
			
			@Override
			public void visit(SignedExpression se) {
				se.getExpression().accept(this);
			}
			
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
	    };
	    
	    expr.accept(deparser);
	}
	
	
	// for SeqScan for statistics collector
	public static String fullyQualifyFilter(String filter, String table) throws JSQLParserException {
		Expression expr = CCJSqlParserUtil.parseCondExpression(filter);
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
			@Override
			public void visit(Column tableColumn) {
				super.getBuffer().append(table);
				super.getBuffer().append(".");
				super.getBuffer().append(tableColumn.getColumnName());
			
		    }
		};
	
	    StringBuilder b = new StringBuilder();
	    deparser.setBuffer(b);
	    expr.accept(deparser);
	    return b.toString();
	    
	}


}

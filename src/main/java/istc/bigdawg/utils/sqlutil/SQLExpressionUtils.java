package istc.bigdawg.utils.sqlutil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.extract.logical.SQLExpressionHandler;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

public class SQLExpressionUtils {
	
	private static Pattern quotedNumbers = Pattern.compile("'[0-9]+'"); 

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


	public static void removeExcessiveParentheses(Expression expr) {
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
		    public void visit(Parenthesis parenthesis) {
				while (parenthesis.getExpression() instanceof Parenthesis)
					parenthesis.setExpression(((Parenthesis)parenthesis.getExpression()).getExpression());
				parenthesis.getExpression().accept(this);				
		    }
			
			@Override
		    public void visit(Multiplication multiplication) {
		        Expression e = multiplication.getLeftExpression();
		        if (e instanceof Parenthesis && ( ((Parenthesis) e).getExpression() instanceof Multiplication || ((Parenthesis) e).getExpression() instanceof Division))
		        	multiplication.setLeftExpression(((Parenthesis) e).getExpression());
		        
		        e = multiplication.getRightExpression();
		        if (e instanceof Parenthesis && ( ((Parenthesis) e).getExpression() instanceof Multiplication || ((Parenthesis) e).getExpression() instanceof Division)) 
		        	multiplication.setRightExpression(((Parenthesis) e).getExpression());
		        
		        multiplication.getLeftExpression().accept(this);
		        multiplication.getRightExpression().accept(this);
		    }
			
			@Override
		    public void visit(Division division) {
				Expression e = division.getLeftExpression();
		        if (e instanceof Parenthesis && ( ((Parenthesis) e).getExpression() instanceof Multiplication || ((Parenthesis) e).getExpression() instanceof Division))
		        	division.setLeftExpression(((Parenthesis) e).getExpression());
		        
		        e = division.getRightExpression();
		        if (e instanceof Parenthesis && ( ((Parenthesis) e).getExpression() instanceof Multiplication || ((Parenthesis) e).getExpression() instanceof Division)) 
		        	division.setRightExpression(((Parenthesis) e).getExpression());
		        
		        division.getLeftExpression().accept(this);
		        division.getRightExpression().accept(this);
		    }
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				
				Expression e = expression.getLeftExpression();
		        if (e instanceof Parenthesis && ((Parenthesis) e).getExpression() instanceof Column)
		        	expression.setLeftExpression(((Parenthesis) e).getExpression());
		        
		        e = expression.getRightExpression();
		        if (e instanceof Parenthesis && ((Parenthesis) e).getExpression() instanceof Column) 
		        	expression.setRightExpression(((Parenthesis) e).getExpression());
		        
		        expression.getLeftExpression().accept(this);
		        expression.getRightExpression().accept(this);
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
				
				Expression e = binaryExpression.getLeftExpression();
		        if (e instanceof Parenthesis && ((Parenthesis) e).getExpression() instanceof Column)
		        	binaryExpression.setLeftExpression(((Parenthesis) e).getExpression());
		        
		        e = binaryExpression.getRightExpression();
		        if (e instanceof Parenthesis && ((Parenthesis) e).getExpression() instanceof Column) 
		        	binaryExpression.setRightExpression(((Parenthesis) e).getExpression());
		        
		        binaryExpression.getLeftExpression().accept(this);
		        binaryExpression.getRightExpression().accept(this);
			}
			
			@Override
		    public void visit(ExpressionList expressionList) {
				List<Expression> exprs = expressionList.getExpressions();
		        for (int i = 0; i < exprs.size(); i++) {
		            while (exprs.get(i) instanceof Parenthesis)
						exprs.set(i, (((Parenthesis)exprs.get(i)).getExpression()));
		            exprs.get(i).accept(this);
		        }
		    }
			
			@Override
			public void visit(Function function) {
				if (!function.isAllColumns())
					function.getParameters().accept(this);
			}
			
			@Override
			public void visit(SignedExpression se) {
				while (se.getExpression() instanceof Parenthesis)
					se.setExpression(((Parenthesis)se.getExpression()).getExpression());
				se.getExpression().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
				
				if (caseExpression.getSwitchExpression() != null) {
					while (caseExpression.getSwitchExpression() instanceof Parenthesis)
						caseExpression.setSwitchExpression(((Parenthesis)caseExpression.getSwitchExpression()).getExpression());
					
					caseExpression.getSwitchExpression().accept(this);
				}
				
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	
		        	Expression e = caseExpression.getWhenClauses().get(i);
		        	while (e instanceof Parenthesis)
		        		caseExpression.getWhenClauses().set(i, ((Parenthesis)caseExpression.getSwitchExpression()));
		        }
		        if (caseExpression.getElseExpression() != null) {
		        	while (caseExpression.getElseExpression() instanceof Parenthesis)
		        		caseExpression.setElseExpression(((Parenthesis)caseExpression.getSwitchExpression()));
		        	caseExpression.getElseExpression().accept(this);
		        }
		    }
			
			@Override public void visit(Column tableColumn) {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
		
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
	
public static List<String> getColumnNamesInAllForms(Expression expr) throws JSQLParserException {
		
		final List<String> attributes = new ArrayList<String>();
	
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				attributes.add(tableColumn.getTable().getName());
				if (tableColumn.getTable().getSchemaName() != null)
					attributes.add(tableColumn.getTable().getSchemaName()+"."+tableColumn.getTable().getName());
				attributes.add(tableColumn.getTable().getFullyQualifiedName());
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {
		        parenthesis.getExpression().accept(this);
		    }
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				expression.getLeftExpression().accept(this);
				expression.getRightExpression().accept(this);
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
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        caseExpression.getSwitchExpression().accept(this);
		        for (Expression exp : caseExpression.getWhenClauses()) exp.accept(this);
		        if (caseExpression.getElseExpression() != null) 
		        	caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
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
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				expression.getLeftExpression().accept(this);
				expression.getRightExpression().accept(this);
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
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        caseExpression.getSwitchExpression().accept(this);
		        for (Expression exp : caseExpression.getWhenClauses()) exp.accept(this);
		        if (caseExpression.getElseExpression() != null) 
		        	caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
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
	
	
	public static String removeExpressionDataTypeArtifactAndConvertLike(String expr) {
		
		expr = expr.replaceAll("::((numeric)|(text)|(bpchar\\[\\])|(bpchar)|(timestamp without time zone)|(date)|(double precision))", "").replaceAll("~~", "like");
		Matcher m = quotedNumbers.matcher(expr);
		while (m.find()) {
			expr = m.replaceFirst(expr.substring(m.start()+1, m.end()-1));
			m.reset(expr);
		}
		
		return expr;
	}


}

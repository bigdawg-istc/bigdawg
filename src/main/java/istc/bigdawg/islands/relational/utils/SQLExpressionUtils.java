package istc.bigdawg.islands.relational.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import istc.bigdawg.islands.relational.SQLExpressionHandler;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;

public class SQLExpressionUtils {
	
	private static Pattern quotedNumbers = Pattern.compile("'[0-9]+'"); 
	private static int subSelectCount = 0;
	private static final String subSelectToken = "BIGDAWGSUBSELECT_";

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
		    public void visit(InExpression ine) {
				while (ine.getLeftExpression() instanceof Parenthesis)
					ine.setLeftExpression(((Parenthesis)ine.getLeftExpression()).getExpression());
				ine.getLeftExpression().accept(this);
				if (ine.getLeftItemsList() != null) ine.getLeftItemsList().accept(this);
				if (ine.getRightItemsList() != null) ine.getRightItemsList().accept(this);
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
			public void visit(MultiExpressionList multiExprList) {
				List<ExpressionList> explist = multiExprList.getExprList();
				for (ExpressionList el : explist) el.accept(this);
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
		        	Expression e = ((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression();
		        	while (e instanceof Parenthesis) e = ((Parenthesis) e).getExpression();
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).setWhenExpression(e);
		        	e.accept(this);
		        	
		        	e = ((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression();
		        	while (e instanceof Parenthesis) e = ((Parenthesis) e).getExpression();
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).setThenExpression(e);
		        	e.accept(this);
		        }
		        if (caseExpression.getElseExpression() != null) {
		        	while (caseExpression.getElseExpression() instanceof Parenthesis)
		        		caseExpression.setElseExpression(((Parenthesis)caseExpression.getSwitchExpression()));
		        	caseExpression.getElseExpression().accept(this);
		        }
		    }
			
			@Override 
			public void visit(IsNullExpression inv) {
				while (inv.getLeftExpression() instanceof Parenthesis)
					inv.setLeftExpression(((Parenthesis)inv.getLeftExpression()).getExpression());
				inv.getLeftExpression().accept(this);
			};
			
			@Override 
			public void visit(AnalyticExpression ae) {
				while (ae.getExpression() instanceof Parenthesis)
					ae.setExpression(((Parenthesis)ae.getExpression()).getExpression());
				if (ae.getPartitionExpressionList() != null) ae.getPartitionExpressionList().accept(this);
			}
			
			@Override public void visit(IntervalExpression ie)  {}
			@Override public void visit(Column tableColumn) {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    deparser.setBuffer(new StringBuilder());
	    expr.accept(deparser);
		
	}
	
	public static void replaceColumnName(Expression expr, boolean restore) {
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override 
			public void visit(Column tableColumn) {
				if (tableColumn.getTable() != null) {
					if (restore) {
						tableColumn.setColumnName(tableColumn.getColumnName().replaceAll("^.*___", ""));
					} else {
						tableColumn.setColumnName(tableColumn.getFullyQualifiedName().replaceAll("[\\.]", "___"));
					}
				}
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {parenthesis.getExpression().accept(this);}
			
			@Override
		    public void visit(InExpression ine) {
				ine.getLeftExpression().accept(this);
				if (ine.getLeftItemsList() != null) ine.getLeftItemsList().accept(this);
				if (ine.getRightItemsList() != null) ine.getRightItemsList().accept(this);
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
				List<Expression> exprs = expressionList.getExpressions();
		        for (int i = 0; i < exprs.size(); i++) exprs.get(i).accept(this);
		    }
			
			@Override
			public void visit(MultiExpressionList multiExprList) {
				List<ExpressionList> explist = multiExprList.getExprList();
				for (ExpressionList el : explist) el.accept(this);
			}
			
			@Override
			public void visit(Function function) {
				if (!function.isAllColumns()) function.getParameters().accept(this);
			}
			
			@Override
			public void visit(SignedExpression se) {
				se.getExpression().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
				if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
				
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override 
			public void visit(IsNullExpression inv) {inv.getLeftExpression().accept(this);}
			
			@Override 
			public void visit(AnalyticExpression ae) {
				if (ae.getPartitionExpressionList() != null) ae.getPartitionExpressionList().accept(this);
			}
			
			@Override public void visit(IntervalExpression ie)  {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
		
	}
	
	public static List<Column> getAttributes(Expression expr) throws JSQLParserException {
		
		final List<Column> attributes = new ArrayList<>();

		SQLExpressionHandler deparser = new SQLExpressionHandler() {
		
			@Override
			public void visit(Column tableColumn) {
				super.visit(tableColumn);
				attributes.add(tableColumn);
			}
			
	    };
	    
	    StringBuilder b = new StringBuilder();
	    deparser.setBuffer(b);
	    expr.accept(deparser);
	    
		return attributes;
	}
	
	public static List<String> getColumnTableNamesInAllForms(Expression expr) throws JSQLParserException {
		
		final Set<String> attributes = new HashSet<String>();
	
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
				if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
	    return new ArrayList<>(attributes);
	}
	
	public static List<String> getColumnNamesInAllForms(Expression expr) throws JSQLParserException {
		
		final Set<String> attributes = new HashSet<String>();
	
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				attributes.add(tableColumn.getColumnName());
				attributes.add(tableColumn.getFullyQualifiedName());
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {
		        parenthesis.getExpression().accept(this);
		    }
			
			@Override
			public void visit(InExpression inExpression) {
				inExpression.getLeftExpression().accept(this);
				if (inExpression.getLeftItemsList() != null) inExpression.getLeftItemsList().accept(this);
				if (inExpression.getRightItemsList() != null) inExpression.getRightItemsList().accept(this);
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
				if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
	    return new ArrayList<>(attributes);
	}

	public static String getRelevantFilterSections(Expression expr, Map<String, String> leftNames, Map<String, String> rightNames) throws JSQLParserException {

		final Set<String> filters = new HashSet<>();

		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			private void processBinary(String original, Expression left, Expression right, String operator) {
				
				try {
					
					List <String> llc = getAttributes(left).stream().filter(d -> d.getTable() != null).map(d -> d.getTable().toString()).collect(Collectors.toList());
					List <String> rlc = getAttributes(right).stream().filter(d -> d.getTable() != null).map(d -> d.getTable().toString()).collect(Collectors.toList());

					Set<String> allLeftNames = new HashSet<>(leftNames.keySet());
					Set<String> allRightNames = new HashSet<>(rightNames.keySet());
					allLeftNames.addAll(leftNames.values());
					allRightNames.addAll(rightNames.values());

					boolean lcl = !Collections.disjoint(llc, allLeftNames);
					boolean rcr = !Collections.disjoint(rlc, allRightNames);
					boolean lcr = !Collections.disjoint(llc, allRightNames);
					boolean rcl = !Collections.disjoint(rlc, allLeftNames);

					if (lcl && rcr || lcr && rcl) {
						filters.add(original);
					} else {
						if (lcl && lcr) {
							left.accept(this);
						} else if (rcl && rcr) {
							right.accept(this);
						} else {
							if ((lcl || lcr) && rlc.isEmpty()) {
								filters.add(original);
							}
							if ((rcl || rcr) && llc.isEmpty()) {
								filters.add(original);
							}
						}
					}
				} catch (JSQLParserException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {

				processBinary(expression.toString(), expression.getLeftExpression(), expression.getRightExpression(), operator);


			}

			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
				processBinary(expression.toString(), expression.getLeftExpression(), expression.getRightExpression(), operator);
			}

			@Override
			public void visit(CaseExpression caseExpression) {
				if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
				for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
					((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
					((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
				}
				if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
			}

			@Override
			public void visit(InExpression inExpression) {
				inExpression.getLeftExpression().accept(this);
				if (inExpression.getLeftItemsList() != null) inExpression.getLeftItemsList().accept(this);
				if (inExpression.getRightItemsList() != null) inExpression.getRightItemsList().accept(this);
			}

			@Override public void visit(ExpressionList expressionList) {for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) iter.next().accept(this);}
			@Override public void visit(Parenthesis parenthesis) {parenthesis.getExpression().accept(this);}
			@Override public void visit(Function function) {function.getParameters().accept(this);}
			@Override public void visit(SignedExpression se) {se.getExpression().accept(this);}
			@Override public void visit(Column tableColumn) {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
	    
	    return String.join(" AND ", filters);
	}
	
	public static List<Expression> getFlatExpressions(Expression expr) throws JSQLParserException {
		
		final List<Expression> filters = new ArrayList<>();
	
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				filters.add(expression);
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
				if (operator.equals(" AND ") || operator.equals(" OR ")){
					expression.getLeftExpression().accept(this);
					expression.getRightExpression().accept(this);
				} else {
					filters.add(expression);
				}
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
				if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override
			public void visit(InExpression inExpression) {
				inExpression.getLeftExpression().accept(this);
				if (inExpression.getLeftItemsList() != null) inExpression.getLeftItemsList().accept(this);
				if (inExpression.getRightItemsList() != null) inExpression.getRightItemsList().accept(this);
			}
			
			@Override public void visit(ExpressionList expressionList) {for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) iter.next().accept(this);}
			@Override public void visit(Parenthesis parenthesis) {parenthesis.getExpression().accept(this);}
			@Override public void visit(Function function) {function.getParameters().accept(this);}
			@Override public void visit(SignedExpression se) {se.getExpression().accept(this);}
			@Override public void visit(Column tableColumn) {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
	    
	    return filters;
	}
	
	public static void renameAttributes(Expression expr, Set<String> originalAliasSet, Set<String> aliasesAndNames, String replacement) throws JSQLParserException {
		
		List<Boolean> functionFlag = new ArrayList<>();
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				
				if (!functionFlag.isEmpty() && aliasesAndNames != null && aliasesAndNames.contains(tableColumn.getTable().getName()))
					tableColumn.getTable().setName(replacement);
				else if (originalAliasSet == null || (tableColumn.getTable() != null && originalAliasSet.contains(tableColumn.getTable().getName())))
					tableColumn.getTable().setName(replacement);
				else if (tableColumn.getTable() == null || tableColumn.getTable().getName() == null)
					tableColumn.setTable(new Table(replacement));
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
				functionFlag.add(true);
				if (!function.isAllColumns()) function.getParameters().accept(this);
			}
			
			@Override
			public void visit(SignedExpression se) {
				se.getExpression().accept(this);
			}
			
			@Override
			public void visit(IsNullExpression ine) {
				ine.getLeftExpression().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
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
		
		expr = SQLUtilities.parseString(expr.replaceAll("::((numeric)|(text)|(bpchar\\[\\])|(bpchar)|(timestamp without time zone)|(date)|(double precision))", ""));
		Matcher m = quotedNumbers.matcher(expr);
		while (m.find()) {
			expr = m.replaceFirst(expr.substring(m.start()+1, m.end()-1));
			m.reset(expr);
		}
		
		return expr;
	}
	
	public static Set<String> getAllTableNamesForSignature(Expression expr, Map<String, String> aliasMapping) {
		Set<String> ret = new HashSet<>();
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				if (tableColumn.getTable() != null && tableColumn.getTable().getName() != null) {
					if (!(tableColumn.getTable().getName().startsWith("BIGDAWGAGGREGATE") || tableColumn.getTable().getName().startsWith("BIGDAWGPRUNED"))) {
						
						if (aliasMapping.get(tableColumn.getTable().getName()) == null) 
							ret.add(tableColumn.getTable().getName()); 
						else 
							ret.add(aliasMapping.get(tableColumn.getTable().getName()));
					}
						
				} else if (aliasMapping.size() == 1) {
					String alias = ((String)aliasMapping.keySet().toArray()[0]);
					if (tableColumn.getTable() == null) tableColumn.setTable(new Table(alias));
					else tableColumn.getTable().setName(alias);
				}
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {
		        parenthesis.getExpression().accept(this);
		    }
			
			@Override
		    public void visit(IsNullExpression ine) {
		        ine.getLeftExpression().accept(this);
		    }
			
			@Override
		    public void visit(InExpression in) {
		        in.getLeftExpression().accept(this);
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
				if (function.getParameters() != null) function.getParameters().accept(this);
			}
			
			@Override
			public void visit(SignedExpression se) {
				se.getExpression().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(IntervalExpression ie) {};
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
		
		return ret;
	}
	
	public static void restoreTableNamesFromAliasForSignature(Expression expr, Map<String, String> aliasMapping) {
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				if (tableColumn.getTable() != null && tableColumn.getTable().getName() != null) {
					if ( tableColumn.getTable().getName().matches("^BIGDAWG.*AGGREGATE_"))
						tableColumn.getTable().setName("BIGDAWGAGGREGATE");
					else if ( tableColumn.getTable().getName().matches("^BIGDAWG.*PRUNED_")) 
						tableColumn.getTable().setName("BIGDAWGPRUNED");
					else if (aliasMapping.get(tableColumn.getTable().getName()) != null)
						tableColumn.getTable().setName(aliasMapping.get(tableColumn.getTable().getName()));
				} else if (aliasMapping.size() == 1) {
					
					String alias = ((String)aliasMapping.keySet().toArray()[0]);
					if (tableColumn.getTable() == null) tableColumn.setTable(new Table(alias));
					else tableColumn.getTable().setName(alias);
				}
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {
		        parenthesis.getExpression().accept(this);
		    }
			
			@Override
		    public void visit(IsNullExpression ine) {
		        ine.getLeftExpression().accept(this);
		    }
			
			@Override
			public void visit(InExpression in) {
				if ( in.getLeftExpression() != null )
					in.getLeftExpression().accept(this);
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
				
				if (function.getParameters() != null) function.getParameters().accept(this);
			}
			
			@Override
			public void visit(SignedExpression se) {
				se.getExpression().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(IntervalExpression ie) {};
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
	}
	
	public static boolean containsArtificiallyConstructedTables(Expression expr) {
		
		List<Boolean> ret = new ArrayList<>();
		ret.add(false);
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
			public void visit(Column tableColumn) {
				if (tableColumn.getTable() != null && tableColumn.getTable().getName() != null) {
					if ( tableColumn.getTable().getName().startsWith("BIGDAWG"))
						ret.set(0, true);
				} 
			}
			
			@Override public void visit(Parenthesis parenthesis) {parenthesis.getExpression().accept(this);}
			@Override public void visit(IsNullExpression ine) {ine.getLeftExpression().accept(this);}
			@Override public void visit(InExpression in) {if ( in.getLeftExpression() != null ) in.getLeftExpression().accept(this);}
			
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
			
			@Override public void visit(Function function) {if (function.getParameters() != null) function.getParameters().accept(this); }
			
			@Override public void visit(SignedExpression se) {se.getExpression().accept(this);}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(IntervalExpression ie) {};
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
	    return ret.get(0);
	}

	/** 
	 * This is supposed to be run only once per query, for cleaning up the original query of SubSelects
	 * @param query
	 * @return
	 * @throws JSQLParserException
	 */
	@Deprecated
	public static String rewriteSubSelectIntoWith(String query) throws JSQLParserException {
		Select s = (Select) CCJSqlParserUtil.parse(query);
		
		List<WithItem> wil = s.getWithItemsList();
		if (wil == null)  wil = new ArrayList<>();
		List<WithItem> wilAddition = new ArrayList<>();
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
			
			private Expression processExpressionSubSelect(SubSelect ss) {
				Column c = new Column();
				WithItem newWI = new WithItem();
				
				String newName; 
				if (ss.getAlias() != null)
					newName = ss.getAlias().getName();
				else 
					newName = getNewSubSelectToken();
				
				c.setColumnName(newName);
				newWI.setName(newName);
				
				
				newWI.setSelectBody(ss.getSelectBody());
				wilAddition.add(newWI);
				return c;
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {
				if (parenthesis.getExpression() instanceof SubSelect) {
					parenthesis.setExpression(
							processExpressionSubSelect((SubSelect)parenthesis.getExpression()));
				} else 
					parenthesis.getExpression().accept(this);
		    }
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				
				if (expression.getLeftExpression() instanceof SubSelect) {
					expression.setLeftExpression(
							processExpressionSubSelect((SubSelect)expression.getLeftExpression()));
				} else 
					expression.getLeftExpression().accept(this);
				
				if (expression.getRightExpression() instanceof SubSelect) {
					expression.setRightExpression(
							processExpressionSubSelect((SubSelect)expression.getRightExpression()));
				} else 
					expression.getRightExpression().accept(this);
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
		        
		        if (binaryExpression.getLeftExpression() instanceof SubSelect) {
		        	binaryExpression.setLeftExpression(
							processExpressionSubSelect((SubSelect)binaryExpression.getLeftExpression()));
				} else 
					binaryExpression.getLeftExpression().accept(this);
				
				if (binaryExpression.getRightExpression() instanceof SubSelect) {
					binaryExpression.setRightExpression(
							processExpressionSubSelect((SubSelect)binaryExpression.getRightExpression()));
				} else 
					binaryExpression.getRightExpression().accept(this);
			}
			
			@Override
		    public void visit(ExpressionList expressionList) {
		        for (int i = 0; i < expressionList.getExpressions().size(); i++) {

		        	Expression expression = expressionList.getExpressions().get(i);
		            if (expression instanceof SubSelect) {
		            	expressionList.getExpressions().set(i, processExpressionSubSelect((SubSelect)expression));
					} else 
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
				
				if (se.getExpression() instanceof SubSelect) {
					se.setExpression(
							processExpressionSubSelect((SubSelect)se.getExpression()));
				} else 
					se.getExpression().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
				
				if (caseExpression.getSwitchExpression() != null && caseExpression.getSwitchExpression() instanceof SubSelect) {
					caseExpression.setSwitchExpression(processExpressionSubSelect((SubSelect)caseExpression.getSwitchExpression()));
				} else caseExpression.getSwitchExpression().accept(this);
				
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	Expression e = ((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression();
		        	if (e instanceof SubSelect) ((WhenClause)caseExpression.getWhenClauses().get(i)).setWhenExpression(processExpressionSubSelect((SubSelect)e));
		        	else e.accept(this);
		        	e = ((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression();
		        	if (e instanceof SubSelect) ((WhenClause)caseExpression.getWhenClauses().get(i)).setThenExpression(processExpressionSubSelect((SubSelect)e));
		        	else e.accept(this);
		        }
		        
		        
		        if (caseExpression.getElseExpression() != null && caseExpression.getElseExpression() instanceof SubSelect) {
					caseExpression.setElseExpression(processExpressionSubSelect((SubSelect)caseExpression.getElseExpression()));
				} else caseExpression.getElseExpression().accept(this);
		    }
			
			@Override
			public void visit(SubSelect subSelect) {
				System.out.println("-- SubSelect Hit: "+subSelect.getAlias());
			}
			
			@Override public void visit(Column tableColumn) {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
		
		SelectVisitor sv = new SelectVisitor() {

			@Override
			public void visit(PlainSelect plainSelect) {
				plainSelect.getWhere().accept(deparser);
			}

			@Override
			public void visit(WithItem withItem) {
				// process with, add to wil Addition
				withItem.getSelectBody().accept(this);
			}
			
			@Override public void visit(SetOperationList setOpList) {}
		};
		
		for (WithItem wi : wil) wi.accept(sv);
		s.getSelectBody().accept(sv);
		
		wil.addAll(0, wilAddition);
		s.setWithItemsList(wil);
		return s.toString();
	}
	
	public static String parseCondForTree(Expression e) throws JSQLParserException{
		StringBuilder sb = new StringBuilder();
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
			
			@Override
		    public void visit(Parenthesis parenthesis) {
				sb.append("{");
		        parenthesis.getExpression().accept(this);
		        sb.append('}');
		    }
			
			@Override
		    public void visit(InExpression ine) {
				sb.append("{in");
		        if (ine.getLeftExpression() != null) 
		        	ine.getLeftExpression().accept(this);
		        if (ine.getLeftItemsList() != null) {
		        	sb.append("{itemlist");
		        	ine.getLeftItemsList().accept(this);
		        	sb.append('}');
		        }
		        if (ine.getRightItemsList() != null) {
		        	sb.append("{itemlist");
		        	ine.getRightItemsList().accept(this);
		        	sb.append('}');
		        }
		        sb.append('}');
		    }
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				sb.append('{').append(operator.trim());
				expression.getLeftExpression().accept(this);
				expression.getRightExpression().accept(this);
				sb.append('}');
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
				sb.append('{').append(operator.trim());
				binaryExpression.getLeftExpression().accept(this);
		        binaryExpression.getRightExpression().accept(this);
		        sb.append('}');
			}
			
			@Override
		    public void visit(ExpressionList expressionList) {
//				sb.append("{ExpressionList");
		        for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
		            Expression expression = iter.next();
		            expression.accept(this);
		        }
//		        sb.append('}');
		    }
			
			@Override
			public void visit(Function function) {
				sb.append('{').append(function.getName()); 
				if ( function.isAllColumns() ) sb.append("{*}");
				if ( function.getParameters() != null) function.getParameters().accept(this);
				sb.append('}');
			}
			
			@Override
			public void visit(SignedExpression se) {
				sb.append("{Signed"); 
				se.getExpression().accept(this);
				sb.append('}');
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
				sb.append("{case");
		        if (caseExpression.getSwitchExpression() != null) {
		        	sb.append("{switch");
		        	caseExpression.getSwitchExpression().accept(this);
		        	sb.append('}');
		        }
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	sb.append("{when");
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	sb.append("}{then");
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);
		        	sb.append('}');
		        }
		        if (caseExpression.getElseExpression() != null) {
		        	sb.append("{else");
		        	caseExpression.getElseExpression().accept(this);
		        	sb.append('}');
		        }
		        sb.append('}');
		    }
			
			@Override 
			public void visit(IsNullExpression ine) {
				if (ine.isNot()) 
					sb.append("{isNotNull");
				else
					sb.append("{isNull");
				ine.getLeftExpression().accept(this);
				sb.append('}');
			}
			
			@Override public void visit(IntervalExpression ie) {sb.append("{interval{").append(ie.getParameter()).append('}').append('}');};
			@Override public void visit(Column tableColumn) {sb.append('{').append(tableColumn.getFullyQualifiedName()).append('}');}
			@Override public void visit(LongValue lv) {sb.append('{').append(lv.getStringValue()).append('}');};
			@Override public void visit(DoubleValue lv) {sb.append('{').append(lv.getValue()).append('}');};
			@Override public void visit(HexValue lv) {sb.append('{').append(lv.getValue()).append('}');};
			@Override public void visit(NullValue lv) {sb.append('{').append(lv.toString()).append('}');};
			@Override public void visit(TimeValue lv) {sb.append('{').append(lv.getValue().toString()).append('}');};
			@Override public void visit(StringValue sv) {sb.append('{').append('\'').append(sv.getValue().replaceAll(":", "")).append('\'').append('}');};
		};
		
		e.accept(deparser);
		
		return sb.toString();
	}
	
	public static boolean isFunctionPresentInCondExpression(Expression e) {
		List<Boolean> ret = new ArrayList<>();
		ret.add(false);
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override public void visit(Function function) {ret.set(0, true);}
			@Override public void visit(Parenthesis parenthesis) {parenthesis.getExpression().accept(this);}
			@Override public void visit(IsNullExpression ine) {ine.getLeftExpression().accept(this);}
			@Override public void visit(SignedExpression se) {se.getExpression().accept(this);}
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				expression.getLeftExpression().accept(this);
				expression.getRightExpression().accept(this);
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
		        expression.getLeftExpression().accept(this);
				expression.getRightExpression().accept(this);
			}
			
			@Override
			public void visit(InExpression ie) {
				ie.getLeftExpression().accept(this);
				if (ie.getLeftItemsList() != null) ie.getLeftItemsList().accept(this);
				if (ie.getRightItemsList() != null) ie.getRightItemsList().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        if (caseExpression.getSwitchExpression() != null) caseExpression.getSwitchExpression().accept(this);
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) caseExpression.getElseExpression().accept(this);
		    }
			
			@Override public void visit(IntervalExpression ie) {};
			@Override public void visit(Column tableColumn) {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
			@Override public void visit(ExpressionList expressionList) {}
	    };
		
		e.accept(deparser);
		
		return ret.get(0);
	}
	
	public static List<Expression> locateFunctionInCondExpression(Expression e){
		
		List<Expression> result = new ArrayList<>();
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
		    public void visit(Parenthesis parenthesis) {
				if (parenthesis.getExpression() instanceof Function) {
					result.add(parenthesis.getExpression());
					result.add(parenthesis);
				}
				else parenthesis.getExpression().accept(this);
		    }
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				
				if (expression.getLeftExpression() instanceof Function) {
					result.add(expression.getLeftExpression());
					result.add(expression);
					return;
				} else expression.getLeftExpression().accept(this);
				
				if (expression.getRightExpression() instanceof Function) {
					result.add(expression.getRightExpression());
					result.add(expression);
				} else expression.getRightExpression().accept(this);
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
		        if (expression.getLeftExpression() instanceof Function) {
					result.add(expression.getLeftExpression());
					result.add(expression);
					return;
				} else expression.getLeftExpression().accept(this);
				
				if (expression.getRightExpression() instanceof Function) {
					result.add(expression.getRightExpression());
					result.add(expression);
				} else expression.getRightExpression().accept(this);
			}
			
			@Override
			public void visit(SignedExpression se) {
				if (se.getExpression() instanceof Function) {
					result.add(se.getExpression());
					result.add(se);
				}
				else se.getExpression().accept(this);
			}
			
			@Override
			public void visit(InExpression ie) {
				if (ie.getLeftExpression() instanceof Function) {
					result.add(ie.getLeftExpression());
					result.add(ie);
				} else ie.getLeftExpression().accept(this);
				
				if (ie.getLeftItemsList() != null) ie.getLeftItemsList().accept(this);
				if (ie.getRightItemsList() != null) ie.getRightItemsList().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        if (caseExpression.getSwitchExpression() != null) {
		        	caseExpression.getSwitchExpression().accept(this);
		        }
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getWhenExpression().accept(this);
		        	((WhenClause)caseExpression.getWhenClauses().get(i)).getThenExpression().accept(this);;
		        }
		        if (caseExpression.getElseExpression() != null) {
		        	Expression el = caseExpression.getElseExpression();
		        	if (el instanceof Function) {
						result.add(el);
						result.add(caseExpression);
					}
					else el.accept(this);
		        }
		    }
			
			@Override
			public void visit(IsNullExpression ine) {
				if (ine.getLeftExpression() instanceof Function) {
					result.add(ine.getLeftExpression());
					result.add(ine);
				}
				else ine.getLeftExpression().accept(this);
			}
			
			@Override public void visit(IntervalExpression ie) {};
			@Override public void visit(Column tableColumn) {};
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
			@Override public void visit(ExpressionList expressionList) {}
			@Override public void visit(Function function) {}
	    };
		
		e.accept(deparser);
		return result;
	}
	
	public static void updateFunctionInCondExpression(Expression substitution, Expression parent){
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			@Override
		    public void visit(Parenthesis parenthesis) {
				parenthesis.setExpression(substitution);
		    }
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				if (expression.getLeftExpression() instanceof Function) {
					expression.setLeftExpression(substitution);
				} else if (expression.getRightExpression() instanceof Function || expression.getLeftExpression() instanceof Column) {
					expression.setRightExpression(substitution);
				} 
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
				if (expression.getLeftExpression() instanceof Function) {
					expression.setLeftExpression(substitution);
				} else if (expression.getRightExpression() instanceof Function || expression.getLeftExpression() instanceof Column) {
					expression.setRightExpression(substitution);
				} 
			}
			
			@Override
			public void visit(SignedExpression se) {
				se.setExpression(substitution);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        caseExpression.setElseExpression(substitution);
		    }
			
			@Override public void visit(Column tableColumn) {}
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
			@Override public void visit(ExpressionList expressionList) {}
			@Override public void visit(Function function) {}
	    };
		
		parent.accept(deparser);
	}
	
	public static Expression getOneSideOfBinaryCondExpression(Expression parent, boolean left){
		
		List<Expression> ret = new ArrayList<>();
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
			
			@Override public void visit(Parenthesis p) {p.getExpression().accept(this);}
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				if (left) ret.add(expression.getLeftExpression());
				else ret.add(expression.getRightExpression());
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
				if (left) ret.add(expression.getLeftExpression());
				else ret.add(expression.getRightExpression());
			}
		};
		parent.accept(deparser);
		if (ret.isEmpty()) return null;
		else {
			Expression result = ret.get(0);
			while (result instanceof Parenthesis)
				result = ((Parenthesis) result).getExpression();
			return result;
		}
	}
	
	public static void setOneSideOfBinaryCondExpression(Expression sub, Expression parent, boolean left){
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
			
			@Override public void visit(Parenthesis p) {p.getExpression().accept(this);}
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				if (left) expression.setLeftExpression(sub);
				else expression.setRightExpression(sub);
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
				if (left) expression.setLeftExpression(sub);
				else expression.setRightExpression(sub);
			}
		};
		parent.accept(deparser);
	}
	
	public static Expression stripDownExpressionForSignature(Expression expr) {
		
		if (expr instanceof Function)
			expr = ((Function)expr).getParameters().getExpressions().get(0);
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
	        
			private Expression getFirstExpression(Expression e) {
				return ((Function) e).getParameters().getExpressions().get(0);
			}
			
			@Override
		    public void visit(Parenthesis parenthesis) {
				while (parenthesis.getExpression() instanceof Function)
					parenthesis.setExpression(getFirstExpression(parenthesis.getExpression()));
		        parenthesis.getExpression().accept(this);
		    }
			
			@Override
		    public void visit(InExpression in) {
				while (in.getLeftExpression() instanceof Function)
					in.setLeftExpression(getFirstExpression(in.getLeftExpression()));
		        in.getLeftExpression().accept(this);
		    }
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				while (expression.getLeftExpression() instanceof Function)
					expression.setLeftExpression(getFirstExpression(expression.getLeftExpression()));
				expression.getLeftExpression().accept(this);
				
				while (expression.getRightExpression() instanceof Function)
					expression.setRightExpression(getFirstExpression(expression.getRightExpression()));
				expression.getRightExpression().accept(this);
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
				while (expression.getLeftExpression() instanceof Function)
					expression.setLeftExpression(getFirstExpression(expression.getLeftExpression()));
				expression.getLeftExpression().accept(this);
				
				while (expression.getRightExpression() instanceof Function)
					expression.setRightExpression(getFirstExpression(expression.getRightExpression()));
				expression.getRightExpression().accept(this);
			}
			
			@Override
		    public void visit(ExpressionList expressionList) {
				System.out.println("Reached Expression List; This shouldn't happen");
//		        for (Iterator<Expression> iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
//		            Expression expression = iter.next();
//		            expression.accept(this);
//		        }
		    }
			
			@Override
			public void visit(Function function) {
				System.out.println("Reached Function; This shouldn't happen");
			}
			
			@Override
			public void visit(SignedExpression se) {
				while (se.getExpression() instanceof Function)
					se.setExpression(getFirstExpression(se.getExpression()));
				se.getExpression().accept(this);
			}
			
			@Override
			public void visit(IsNullExpression ine) {
				while (ine.getLeftExpression() instanceof Function)
					ine.setLeftExpression(getFirstExpression(ine.getLeftExpression()));
				ine.getLeftExpression().accept(this);
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
		        if (caseExpression.getSwitchExpression() != null) {
		        	while (caseExpression.getSwitchExpression() instanceof Function)
		        		caseExpression.setSwitchExpression(getFirstExpression(caseExpression.getSwitchExpression()));
		        	caseExpression.getSwitchExpression().accept(this);
		        }
		        for (int i = 0; i < caseExpression.getWhenClauses().size(); i++) {
		        	
		        	WhenClause wc = (WhenClause)caseExpression.getWhenClauses().get(i);
		        	while (wc.getWhenExpression() instanceof Function)
		        		wc.setWhenExpression(getFirstExpression(wc.getWhenExpression()));
		        	wc.getWhenExpression().accept(this);

		        	while (wc.getThenExpression() instanceof Function)
		        		wc.setThenExpression(getFirstExpression(wc.getThenExpression()));
		        	wc.getThenExpression().accept(this);
		        }
		        if (caseExpression.getElseExpression() != null) {
		        	
		        	while (caseExpression.getElseExpression() instanceof Function)
		        		caseExpression.setElseExpression(getFirstExpression(caseExpression.getElseExpression()));
		        	caseExpression.getElseExpression().accept(this);
		        }
		    }
			
			@Override public void visit(IntervalExpression ie){};
			@Override public void visit(Column tableColumn) {};
			@Override public void visit(LongValue lv) {};
			@Override public void visit(DoubleValue lv) {};
			@Override public void visit(HexValue lv) {};
			@Override public void visit(NullValue lv) {};
			@Override public void visit(TimeValue lv) {};
			@Override public void visit(StringValue sv) {};
	    };
	    
	    expr.accept(deparser);
	    return expr;
	}
	
	private static String getNewSubSelectToken() {
		subSelectCount++;
		return subSelectToken + subSelectCount;
	}
	
	public static String getBinaryExpressionOperatorToken(Expression e) {
		String ret = null;
		if (e instanceof EqualsTo) {
			ret = "=";
		} else if (e instanceof LikeExpression) {
			ret = "LIKE";
		} else if (e instanceof InExpression) {
			ret = "IN";
		} else if (e instanceof GreaterThanEquals) {
			ret = ">=";
		} else if (e instanceof GreaterThan) {
			ret = ">";
		} else if (e instanceof MinorThanEquals) {
			ret = "<=";
		} else if (e instanceof MinorThan) {
			ret = "<";
		} else if (e instanceof NotEqualsTo) {
			ret = "<>";
		} else if (e instanceof Between) {
			ret = "BETWEEN";
		} else {
			ret = "UNKNOWN";
			System.out.println("Unknown binary expression operator: "+e.toString());
		};
		return ret;
	}
}

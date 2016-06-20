package istc.bigdawg.islands.PostgreSQL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import istc.bigdawg.islands.PostgreSQL.utils.SQLReferencedAttribute;
import istc.bigdawg.islands.PostgreSQL.utils.SQLReferencedSchema;
import istc.bigdawg.islands.PostgreSQL.utils.SQLStoredAttribute;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class SQLAttributeResolver {

	// takes in the content of a <Item> field in EXPLAIN xml 
	// keeps track of fields referenced in expression for security level 
	// and any function / aggregates referenced
	
	// supports:
	// column references
	// math expression
	// aggregate 
	// windowed aggregate
	
	// TODO: expressions with multiple aggregates might not work
	
	private List<Function> aggregates;
	private List<AnalyticExpression> windowedAggregates;
	private String alias = null; // attr alias
	final SQLReferencedAttribute outAttribute = new SQLReferencedAttribute();
	
	private SQLReferencedAttribute srcAttribute = null; // if the inferred attribute has just one source, record it here
	
	public SQLAttributeResolver(String expr,  SQLReferencedSchema srcSchema, 
			SQLTableExpression supplement) throws JSQLParserException {
		
		aggregates = new ArrayList<Function>();
		windowedAggregates = new ArrayList<AnalyticExpression>();

		if(srcSchema.getAttribute(expr) != null) {
			srcAttribute = srcSchema.getAttribute(expr);
			
			outAttribute.copy(srcAttribute);
			
			if(supplement != null) {
				alias = supplement.getAlias(expr);
				outAttribute.setAlias(alias);
			}
			return;
		}
		
		if(supplement != null) {
			alias = supplement.getAlias(expr);
			outAttribute.setAlias(alias);
		}
		
		// get rid of any psql param placeholders
		expr = expr.replace("?", " ");
		
		Expression parsedExpression = CCJSqlParserUtil.parseExpression(expr);

		
		outAttribute.setName(expr); // its name is an amalgamation of the names of its source attributes, but we refer to it as alias from now on		
		outAttribute.setReplicated(true); // inferred below
		outAttribute.setSecurityPolicy(SQLStoredAttribute.SecurityPolicy.Public);
		outAttribute.setType(null);
		
		
		
		ExpressionDeParser deparser = new ExpressionDeParser() {
		
			
			public void visit(Column tableColumn) {
				super.visit(tableColumn);

				String name = tableColumn.getFullyQualifiedName();
				SQLReferencedAttribute lookup = srcSchema.getAttribute(name);
				outAttribute.addSourceAttribute(lookup);
				
				
				// first column that is in this expression
				if(outAttribute.getType() == null) {
					outAttribute.setType(lookup.getType());
					outAttribute.setReplicated(lookup.getReplicated());
					
				}
				else {
					// check to make sure it is the same type
				    assert(outAttribute.getType().getDataType().equals(lookup.getType().getDataType()));
					
					if(lookup.getReplicated() == false) {
						outAttribute.setReplicated(false);
					}
					
					
				}
			}
			
			// simple aggregates
			public void visit(Function function) { 
				super.visit(function);
				aggregates.add(function);
				if(function.isAllColumns()) {
					// find attribute with highest security attribute in src schema
					// must be count(*), all others don't support this
					// TODO: make more fine-grained by only enforcing security policy of source attrs 
					// that are computed upon
					
					   setUpAggregateAllColumns(srcSchema, outAttribute);
					}   // else (not *)  delegate got column visitor above
			}
		
			// windowed aggregate
			public void visit(AnalyticExpression aexpr) {
				super.visit(aexpr);
				// grab aexpr from supplement
				
				AnalyticExpression fullExpression = supplement.getAnalyticExpression();
				windowedAggregates.add(fullExpression);
			
				assert(aexpr.getName().equals("row_number")); // all others not yet implemented
				setUpAggregateAllColumns(srcSchema, outAttribute);  // TODO: make this more fine grained, only derived from ORDER BY, PARTITION BY and possibly aggregate
				outAttribute.setName(fullExpression.toString());
				String attrAlias = supplement.getAlias(fullExpression.toString());
				if(attrAlias != null) {
					outAttribute.setAlias(attrAlias);
				}
			}
			

		
		
		}; // end expression parser
		
		
		
		StringBuilder b = new StringBuilder();
		deparser.setBuffer(b);
		parsedExpression.accept(deparser); // adjusts outAttribute for winagg case
		  
		
	}
	
	// takes in alias src, determines if it has a match in src schema
	// if so, it prefixes the column reference with the src table
	String fullyQualify(String expr, Map<String, SQLStoredAttribute> srcSchema) throws JSQLParserException {
		
		ExpressionDeParser deparser = new ExpressionDeParser() {
		
			
			public void visit(Column tableColumn) {
				
				if(tableColumn.getTable().getName() == null) {
					
					for(String s : srcSchema.keySet()) {
						final String[] names = s.split("\\.");


						if(tableColumn.getColumnName().equalsIgnoreCase(names[1])) { // happens just once
							final Table t = new Table(names[0]);
							tableColumn.setTable(t);
						}
					}
										
				}
				
				super.visit(tableColumn);
			}
			
		
		}; // end expression parser
		
		  StringBuilder b = new StringBuilder();
		  deparser.setBuffer(b);
  		  Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
		  parseExpression.accept(deparser);
		  return b.toString();

	}
		

	
	
	static void setUpAggregateAllColumns(SQLReferencedSchema srcSchema, SQLReferencedAttribute out) {
		ColDataType intAttrType = new ColDataType();
		intAttrType.setDataType("integer");
		out.setType(intAttrType);
		out.setSecurityPolicy(SQLStoredAttribute.SecurityPolicy.Public);
		out.setReplicated(true);
		
		for(SQLReferencedAttribute src : srcSchema.getAttributes()) {
			out.addSourceAttribute(src);
			 if(src.getReplicated() == false) {
				 out.setReplicated(false);
			 }
		}
		
		
	}
	
	public SQLReferencedAttribute getAttribute() {
		return outAttribute;
	}
	
	public boolean hasAggregate() {
		return !aggregates.isEmpty();
	}
	
	public boolean hasWindowedAggregates() {
		return !windowedAggregates.isEmpty();
	}
	
	public List<Function> getAggregates() {
		return aggregates;
	}
	
	public List<AnalyticExpression> getWindowedAggregates() {
		return windowedAggregates;
	}

	public SQLReferencedAttribute getSourceAttribute() {
		return srcAttribute;
	}

}
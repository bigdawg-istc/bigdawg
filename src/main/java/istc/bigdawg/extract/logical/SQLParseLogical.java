package istc.bigdawg.extract.logical;

import java.util.Iterator;
import java.util.List;

import istc.bigdawg.extract.logical.SQLExpressionHandler;
import istc.bigdawg.extract.logical.SQLHandler;
import istc.bigdawg.plan.SQLQueryPlan;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;

// Extract parts of SQL statement that do not appear in EXPLAIN VERBOSE
// Supplement to extract.plan

public class SQLParseLogical {

    
    private SQLQueryPlan queryPlan;

    
	public SQLParseLogical(String query) throws JSQLParserException {
		Select select = (Select) CCJSqlParserUtil.parse(query);	
		queryPlan = new SQLQueryPlan();
		queryPlan.setLogicalStatement(select);
		

		SQLExpressionHandler expressionSupplement = new SQLExpressionHandler();
		StringBuilder buffer = new StringBuilder();

		
		SQLTableExpression supplement = new SQLTableExpression();
		supplement.setSelect((PlainSelect) select.getSelectBody()); 
		
		SQLHandler deparser = new SQLHandler(expressionSupplement, buffer, supplement);
		expressionSupplement.setSelectVisitor(deparser);
		expressionSupplement.setBuffer(buffer);

		
		
		List<WithItem> withItemsList = select.getWithItemsList();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            buffer.append("WITH ");
            for (Iterator<WithItem> iter = withItemsList.iterator(); iter.hasNext();) {
                    WithItem withItem = (WithItem) iter.next();
                    
            		SQLTableExpression t = new SQLTableExpression();
            		t.setSelect((PlainSelect) withItem.getSelectBody());

            		deparser.setTableSupplement(t);
            		expressionSupplement.setTableSupplement(t);
            		
                    withItem.accept(deparser);
                    queryPlan.addTableSupplement(withItem.getName(), t);
                    
                    if (iter.hasNext()) {
                            buffer.append(",");
                    }
                    buffer.append(" ");
            }
        }

		deparser.setTableSupplement(supplement);
		expressionSupplement.setTableSupplement(supplement);

		select.getSelectBody().accept(deparser);
		queryPlan.addTableSupplement("main", supplement);

		
		
	}
	
	public SQLQueryPlan getSQLQueryPlan() {
		return queryPlan;
	}
	
}

package istc.bigdawg.islands.relational;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.islands.relational.utils.SQLPrepareQuery;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;

// Extract parts of SQL statement that do not appear in EXPLAIN VERBOSE
// Supplement to extract.plan

public class SQLParseLogical {

    
    private SQLQueryPlan queryPlan;

    static Select tryJSONPlaceholderParse(JSQLParserException e, String q) throws QueryParsingException {
        if ((e.getCause().getMessage() != null && e.getCause().getMessage().contains(">")) ||
				(e.getMessage() != null && e.getMessage().contains(">"))) {
            try {
                String newq = SQLJSONPlaceholderParser.transformJSONQuery(q);
                return (Select) CCJSqlParserUtil.parse(newq);
            }
            catch (Exception ex) {
                throw new QueryParsingException(e.getCause().getMessage(), e);
            }
        }
        else {
            throw new QueryParsingException(e.getMessage() == null ? e.getCause().getMessage() : e.getMessage(), e);
        }
    }

	public SQLParseLogical(String query) throws SQLException, BigDawgCatalogException, QueryParsingException {
		
		String q = SQLPrepareQuery.preprocessDateAndTime(query);
		
		Select select;
		try {
			select = (Select) CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
		    select = tryJSONPlaceholderParse(e, q);
		}
		queryPlan = new SQLQueryPlan();
		queryPlan.setLogicalStatement(select);
		

		SQLExpressionHandler expressionSupplement = new SQLExpressionHandler();
		StringBuilder buffer = new StringBuilder();

		
		SQLTableExpression supplement = new SQLTableExpression();
		if (select.getSelectBody() instanceof PlainSelect)
			supplement.setSelect((PlainSelect) select.getSelectBody());
		else if (select.getSelectBody() instanceof SetOperationList)
			supplement.setSelect((PlainSelect)(((SetOperationList)select.getSelectBody()).getSelects().get(0)));
		else 
			throw new QueryParsingException("Unsupported selectBody type: "+select.getSelectBody().getClass().getSimpleName());
			
		
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
		queryPlan.addTableSupplement("main_0", supplement);
		if (select.getSelectBody() instanceof SetOperationList) {
			List<SelectBody> sbs = ((SetOperationList)select.getSelectBody()).getSelects();
			for (int i = 1; i < sbs.size(); ++i ) {
				
				expressionSupplement = new SQLExpressionHandler();
				buffer = new StringBuilder();
				
				supplement = new SQLTableExpression();
				supplement.setSelect((PlainSelect)(sbs.get(i)));
				
				deparser = new SQLHandler(expressionSupplement, buffer, supplement);
				expressionSupplement.setSelectVisitor(deparser);
				expressionSupplement.setBuffer(buffer);
				
				deparser.setTableSupplement(supplement);
				expressionSupplement.setTableSupplement(supplement);

				select.getSelectBody().accept(deparser);
				queryPlan.addTableSupplement("main_"+i, supplement);
			}
		}
		
	}
	
	
	public SQLQueryPlan getSQLQueryPlan() {
		return queryPlan;
	}
	
}

package istc.bigdawg.islands.relational.operators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.relational.SQLTableExpression;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;

public class SQLIslandScan extends SQLIslandOperator implements Scan {
	
	private Expression filterExpression = null;
	private Expression indexCond = null;
	private String srcTable;
	private String operatorName = null;
	
	private String tableAlias;  //may be query-specific, need to derive it here
	protected Table table;
	private boolean hasFunctionInFilterExpression = false;

	public SQLIslandScan(Map<String, String> parameters, List<String> output, SQLIslandOperator child, SQLTableExpression supplement) throws Exception {
		super(parameters, output, child, supplement);

		isBlocking = false;

		setSrcTable(parameters.get("Relation-Name"));
		
		if(getSrcTable() == null) { // it's a cte scan
			setSrcTable(parameters.get("CTE-Name"));
		}
		setTableAlias(parameters.get("Alias"));
		
		if (parameters.get("Filter") != null) {
			
			String s = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(parameters.get("Filter"));
			
			setFilterExpression(CCJSqlParserUtil.parseCondExpression(s));
			SQLExpressionUtils.removeExcessiveParentheses(filterExpression);
			
			setHasFunctionInFilterExpression(SQLExpressionUtils.isFunctionPresentInCondExpression(filterExpression));
//			System.out.println("---> filterExpression: "+filterExpression);
//			
//			filterSet = new HashSet<Expression>();
//			filterSet.add(filterExpression);
			
		}
		
		if (parameters.get("Index-Cond") != null) {
			String s = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(parameters.get("Index-Cond"));
			
			setIndexCond(CCJSqlParserUtil.parseCondExpression(s));
			SQLExpressionUtils.removeExcessiveParentheses(indexCond);
			
			List<Expression> exprs = SQLExpressionUtils.getFlatExpressions(getIndexCond());
			Expression result = null; 
			for (Expression e : exprs) {
				if (SQLExpressionUtils.getAttributes(e).size() == 1) {
					if (filterExpression == null)
						filterExpression = e;
					else 
						filterExpression = new AndExpression(filterExpression, e);
					continue;
				}
				if (result == null)
					result = e; 
				else 
					result = new AndExpression(result, e);
			}
			indexCond = result;
			
//			System.out.println("---> indexCond: "+indexCond);
			
//			if (filterSet == null) filterSet = new HashSet<Expression>();
//			filterSet.add(indexCond);
		}
		
		table = new Table(getSrcTable()); // new one to accommodate aliasing
		if (parameters.get("Schema") != null && (!parameters.get("Schema").equals("public"))) 
			table.setSchemaName(parameters.get("Schema"));

		if(getTableAlias() != null && !getTableAlias().equalsIgnoreCase(getSrcTable())) {
			table.setAlias(new Alias(getTableAlias()));
		}
	}
	
	public SQLIslandScan(SQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		SQLIslandScan sc = (SQLIslandScan) o;
		
		if (sc.getFilterExpression() != null) 
			this.setFilterExpression(CCJSqlParserUtil.parseCondExpression(sc.getFilterExpression().toString()));
		this.setSrcTable(new String(sc.getSrcTable()));
		this.setTableAlias(new String(sc.getTableAlias()));
		this.setHasFunctionInFilterExpression(sc.isHasFunctionInFilterExpression());

		this.table = new Table();
		try {
			this.table.setName(new String(sc.table.getName()));
			if (sc.table.getSchemaName() != null) this.table.setSchemaName(new String(sc.table.getSchemaName()));
			if (sc.table.getAlias() != null) this.table.setAlias(sc.table.getAlias());
			if (sc.table.getASTNode() != null) this.table.setASTNode(sc.table.getASTNode());
			if (sc.table.getDatabase() != null)this.table.setDatabase(sc.table.getDatabase());
			if (sc.table.getPivot() != null)this.table.setPivot(sc.table.getPivot());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Table getTable() {
		return table;
		
	}
	
	
	@Override
	public Map<String, List<String>> getTableLocations(Map<String, List<String>> locations) {
		Map<String, List<String>> result = new HashMap<>();
		if (children != null) {
			for (Operator o : children)
				result.putAll(((SQLIslandOperator) o).getTableLocations(locations));
		}
		String schemaAndName = table.getName();
		if (table.getSchemaName() != null) schemaAndName = table.getSchemaName() + "." +schemaAndName;
		result.put(schemaAndName, locations.get(schemaAndName));
		return result;
	}
	
	@Override
	public Map<String, Expression> getChildrenPredicates() throws Exception {
		Map<String, Expression> ret = new HashMap<>();
		ret.put((this.getTableAlias() != null ? this.getTableAlias() : this.getSrcTable()), getIndexCond());
		return ret;
	}
	
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		
		Operator parent = this;
		while (!parent.isBlocking() && parent.getParent() != null ) parent = parent.getParent();
		Map<String, String> aliasMapping = parent.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = new HashMap<>();
		
		// filter
		if (getFilterExpression() != null && !SQLExpressionUtils.containsArtificiallyConstructedTables(getFilterExpression())) {
			addToOut(CCJSqlParserUtil.parseCondExpression(getFilterExpression().toString()), out, aliasMapping);
		}
		
		// join condition
		if (getIndexCond() != null && !SQLExpressionUtils.containsArtificiallyConstructedTables(getIndexCond())) {
			addToOut(CCJSqlParserUtil.parseCondExpression(getIndexCond().toString()), out, aliasMapping);
		}
		
		return out;
	}
	
	
	
	@Override
	public void seekScanAndProcessAggregateInFilter() throws Exception {
		
		if (getFilterExpression() == null) return;
		
		if (!SQLExpressionUtils.isFunctionPresentInCondExpression(getFilterExpression())) return;
		
		List<Expression> exp = SQLExpressionUtils.locateFunctionInCondExpression(getFilterExpression());
		while (!exp.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			Set<String> names = new HashSet<>();
			Expression result = resolveAggregatesInFilter(exp.get(0).toString(), true, this, names, sb);
			if (result != null) {
				SQLExpressionUtils.updateFunctionInCondExpression(result, exp.get(1));
				exp = SQLExpressionUtils.locateFunctionInCondExpression(getFilterExpression());
				SQLExpressionUtils.renameAttributes(getIndexCond(), names, null, sb.toString());
				
			} else {
				break;
			}
		}
	}

	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}

	public Expression getFilterExpression() {
		return filterExpression;
	}


	public void setFilterExpression(Expression filterExpression) {
		this.filterExpression = filterExpression;
	}


	public boolean isHasFunctionInFilterExpression() {
		return hasFunctionInFilterExpression;
	}


	public void setHasFunctionInFilterExpression(boolean hasFunctionInFilterExpression) {
		this.hasFunctionInFilterExpression = hasFunctionInFilterExpression;
	}


	public String getSrcTable() {
		return srcTable;
	}


	public void setSrcTable(String srcTable) {
		this.srcTable = srcTable;
	}


	public String getTableAlias() {
		return tableAlias;
	}


	public void setTableAlias(String tableAlias) {
		this.tableAlias = tableAlias;
	}


	public String getOperatorName() {
		return operatorName;
	}


	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}


	public Expression getIndexCond() {
		return indexCond;
	}


	public void setIndexCond(Expression indexCond) {
		this.indexCond = indexCond;
	}


	@Override
	public String generateRelevantJoinPredicate(){
		return getIndexCond() != null ? getIndexCond().toString(): null;
	}
	
	@Override
	public String getSourceTableName() {
		return this.srcTable;
	}

	@Override
	public void setSourceTableName(String srcTableName) {
		this.srcTable = srcTableName;
	}

	
	
}

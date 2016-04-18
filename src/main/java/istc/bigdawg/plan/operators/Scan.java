package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;

public class Scan extends Operator {
	
	protected Expression filterExpression = null;
	protected Expression indexCond = null;
	protected String srcTable;
	protected String operatorName = null;
	
	protected String tableAlias;  //may be query-specific, need to derive it here
	protected Table table;
	protected boolean hasFunctionInFilterExpression = false;

	public String getJoinPredicate(){
		if (indexCond != null) return indexCond.toString();
		else return null;
		
	}

	
	public Scan(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception {
		super(parameters, output, child, supplement);

		isBlocking = false;

		srcTable = parameters.get("Relation-Name");
		
		if(srcTable == null) { // it's a cte scan
			srcTable = parameters.get("CTE-Name");
		}
		tableAlias = parameters.get("Alias");
		
		if (parameters.get("Filter") != null) {
			
			String s = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(parameters.get("Filter"));
			
			filterExpression = CCJSqlParserUtil.parseCondExpression(s);
			SQLExpressionUtils.removeExcessiveParentheses(filterExpression);
			
			hasFunctionInFilterExpression = SQLExpressionUtils.isFunctionPresentInCondExpression(filterExpression);
//			System.out.println("---> filterExpression: "+filterExpression);
//			
//			filterSet = new HashSet<Expression>();
//			filterSet.add(filterExpression);
			
		}
		
		if (parameters.get("Index-Cond") != null) {
			String s = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(parameters.get("Index-Cond"));
			
			indexCond = CCJSqlParserUtil.parseCondExpression(s);
			SQLExpressionUtils.removeExcessiveParentheses(indexCond);
			
//			System.out.println("---> indexCond: "+indexCond);
			
//			if (filterSet == null) filterSet = new HashSet<Expression>();
//			filterSet.add(indexCond);
		}
		
		table = new Table(srcTable); // new one to accommodate aliasing
		if (parameters.get("Schema") != null && (!parameters.get("Schema").equals("public"))) 
			table.setSchemaName(parameters.get("Schema"));

		if(tableAlias != null && !tableAlias.equalsIgnoreCase(srcTable)) {
			table.setAlias(new Alias(tableAlias));
		}
	}
	
	// for AFL
	public Scan(Map<String, String> parameters, SciDBArray output, Operator child) throws Exception {
		super(parameters, output, child);

		isBlocking = false;

		srcTable = parameters.get("Relation-Name");
		
		if(srcTable == null) { // it's a cte scan
			srcTable = parameters.get("CTE-Name");
		}
		tableAlias = parameters.get("Alias");
		
		if(parameters.get("Filter") != null) {
			
			filterExpression = CCJSqlParserUtil.parseCondExpression(parameters.get("Filter"));
			hasFunctionInFilterExpression = SQLExpressionUtils.isFunctionPresentInCondExpression(filterExpression);
		}
		
		table = new Table(srcTable); // new one to accommodate aliasing
		if (parameters.get("Schema") != null && (!parameters.get("Schema").equals("public"))) 
			table.setSchemaName(parameters.get("Schema"));

		if(tableAlias != null && !tableAlias.equalsIgnoreCase(srcTable)) {
			table.setAlias(new Alias(tableAlias));
		}

		
	}
	
	public Scan(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Scan sc = (Scan) o;
		
		if (sc.filterExpression != null) 
			this.filterExpression = CCJSqlParserUtil.parseCondExpression(sc.filterExpression.toString());
		this.srcTable = new String(sc.srcTable);
		this.tableAlias = new String(sc.tableAlias);
		this.hasFunctionInFilterExpression = sc.hasFunctionInFilterExpression;

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
	public Select generateSQLStringDestOnly(Select dstStatement, boolean isSubTreeRoot, boolean stopAtJoin, Set<String> allowedScans) throws Exception {

		if(dstStatement == null) {
			dstStatement = SelectUtils.buildSelectFromTable(table);
		}
		
		if (filterExpression != null && !isPruned() && !hasFunctionInFilterExpression) { // FilterExpression with function is pulled
			
			Expression fe = CCJSqlParserUtil.parseCondExpression(filterExpression.toString());
			
			List<Column> cs = SQLExpressionUtils.getAttributes(fe);
			List<String> ss = new ArrayList<>();
			for (Column c : cs)  ss.add(c.getTable().getName());
			ss.remove(this.srcTable);
			if (tableAlias != null) ss.remove(tableAlias);
			ss.removeAll(allowedScans);
			
			
			if (ss.isEmpty()) {
			
				PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
				
				Expression e = null; 
				if(ps.getWhere() != null) {
					e = new AndExpression(ps.getWhere(), fe);
				} else 
					e = fe;
				
				if ( e != null) e = CCJSqlParserUtil.parseCondExpression(e.toString());
				
				try {
					ps.setWhere(e);
				} catch (Exception ex) {
					System.out.println("filterSet exception: "+fe.toString());
				}
			}
		}
		
//		if (indexCond != null ) { // used to have a !isPruned;
//			
//			Expression ic = CCJSqlParserUtil.parseCondExpression(indexCond.toString());
//			
//			List<Column> cs = SQLExpressionUtils.getAttributes(ic);
//			List<String> ss = new ArrayList<>();
//			for (Column c : cs)  ss.add(c.getTable().getName());
//			ss.remove(this.srcTable);
//			if (tableAlias != null) ss.remove(tableAlias);
//			ss.removeAll(allowedScans);
//			
//			
//			if (ss.isEmpty()) {
//				
//				if (isPruned) {
//					Set<String> names = new HashSet<>();
//					names.add(this.srcTable);
//					names.add(this.tableAlias);
//					SQLExpressionUtils.renameAttributes(ic, names, null, this.getPruneToken());
//				} 
//				
//				PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
//				
//				Expression e = null; 
//				if(ps.getWhere() != null) {
//					e = new AndExpression(ps.getWhere(), ic);
//				} else {
//					e = ic;
//				}
//				
//				e = CCJSqlParserUtil.parseCondExpression(e.toString());
//
//				try {
//					ps.setWhere(e);
//				} catch (Exception ex) {
//					System.out.println("indexCond exception: "+ic.toString());
//				}
//			}
//		}
		
		return dstStatement;

		
	}
	
	
	
	@Override
	public String generateAFLString(int recursionLevel) throws Exception {
		StringBuilder sb = new StringBuilder();
		if (!(operatorName.equals("scan") && recursionLevel > 0))
			sb.append(operatorName).append('(');
		
		boolean ped = (!this.getChildren().isEmpty()) && this.getChildren().get(0).isPruned();
		
		if (ped)
			sb.append(this.getChildren().get(0).getPruneToken());
		else {
			if (!operatorName.equals("apply") || children.size() == 0) {
				sb.append(srcTable);
			}
			switch (operatorName) {
			case "apply":
				if (children.size() != 0) {
					sb.append(children.get(0).generateAFLString(recursionLevel + 1));
				} 
				
				for (String s : outSchema.keySet()){
					if (outSchema.get(s).isHidden()) continue;
					if (outSchema.get(s).getName().equals(outSchema.get(s).getExpressionString())) continue;
					sb.append(", ").append(s).append(", ");
					if (ped) {
						// the apply
						Expression expression = CCJSqlParserUtil.parseExpression(outSchema.get(s).getExpressionString());
						Set<String> nameSet = new HashSet<>( SQLExpressionUtils.getColumnTableNamesInAllForms(expression));
						SQLExpressionUtils.renameAttributes(expression, nameSet, nameSet, getChildren().get(0).getPruneToken());
						sb.append(expression.toString());
					} else {
						sb.append(outSchema.get(s).getExpressionString());
					}
				}
				
				break;
			case "project":
				for (String s : outSchema.keySet()){
					if (outSchema.get(s).isHidden()) continue;
					
					sb.append(", ");
					if (ped) {
						String[] o = outSchema.get(s).getName().split("\\.");
						sb.append(getChildren().get(0).getPruneToken()).append('.').append(o[o.length-1]);
					} else 
						sb.append(outSchema.get(s).getName());
				}
				break;
			case "scan":
				break;
			case "filter":
				sb.append(", ").append(filterExpression);
				break;
			default:
				break;
			}
		}
		if (!(operatorName.equals("scan") && recursionLevel > 0))
			sb.append(')');
		return sb.toString();
	}
	
	

	@Override
	public Map<String, List<String>> getTableLocations(Map<String, List<String>> locations) {
		Map<String, List<String>> result = new HashMap<>();
		if (children != null) {
			for (Operator o : children)
				result.putAll(o.getTableLocations(locations));
		}
		String schemaAndName = table.getName();
		if (table.getSchemaName() != null) schemaAndName = table.getSchemaName() + "." +schemaAndName;
		result.put(schemaAndName, locations.get(schemaAndName));
		return result;
	}
	
	@Override
	protected Map<String, Expression> getChildrenIndexConds() throws Exception {
		Map<String, Expression> ret = new HashMap<>();
		ret.put((this.tableAlias != null ? this.tableAlias : this.srcTable), indexCond);
		return ret;
	}
	
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		
		Operator parent = this;
		while (!parent.isBlocking && parent.parent != null ) parent = parent.parent;
		Map<String, String> aliasMapping = parent.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = new HashMap<>();
		
		// filter
		if (filterExpression != null && !SQLExpressionUtils.containsArtificiallyConstructedTables(filterExpression)) {
			addToOut(CCJSqlParserUtil.parseCondExpression(filterExpression.toString()), out, aliasMapping);
		}
		
		// join condition
		if (indexCond != null && !SQLExpressionUtils.containsArtificiallyConstructedTables(indexCond)) {
			addToOut(CCJSqlParserUtil.parseCondExpression(indexCond.toString()), out, aliasMapping);
		}
		
		return out;
	}
	
	
	
	@Override
	public void seekScanAndProcessAggregateInFilter() throws Exception {
		
		if (filterExpression == null) return;
		
		if (!SQLExpressionUtils.isFunctionPresentInCondExpression(filterExpression)) return;
		
		List<Expression> exp = SQLExpressionUtils.locateFunctionInCondExpression(filterExpression);
		while (!exp.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			Set<String> names = new HashSet<>();
			Expression result = resolveAggregatesInFilter(exp.get(0).toString(), true, this, names, sb);
			if (result != null) {
				SQLExpressionUtils.updateFunctionInCondExpression(result, exp.get(1));
				exp = SQLExpressionUtils.locateFunctionInCondExpression(filterExpression);
				SQLExpressionUtils.renameAttributes(indexCond, names, null, sb.toString());
				
			} else {
				break;
			}
		}
	}

}

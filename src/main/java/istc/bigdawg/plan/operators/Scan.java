package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.utils.sqlutil.SQLUtilities;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;

public class Scan extends Operator {
	
	protected String filterExpression = null;
	protected Set<String> filterSet;
	protected String srcTable;
	
	protected String tableAlias;  //may be query-specific, need to derive it here
	protected Table table;

	
	public Scan(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception {
		super(parameters, output, child, supplement);

		isBlocking = false;

		srcTable = parameters.get("Relation-Name");
		
		if(srcTable == null) { // it's a cte scan
			srcTable = parameters.get("CTE-Name");
		}
		tableAlias = parameters.get("Alias");
		
		if(parameters.get("Filter") != null) {
			filterExpression = SQLUtilities.parseString(parameters.get("Filter"));
			filterSet = new HashSet<String>();
			filterSet.add(filterExpression);
		}
		
		table = new Table(srcTable); // new one to accommodate aliasing
		if (parameters.get("Schema") != null && (!parameters.get("Schema").equals("public"))) 
			table.setSchemaName(parameters.get("Schema"));

		if(tableAlias != null && !tableAlias.equalsIgnoreCase(srcTable)) {
			table.setAlias(new Alias(tableAlias));
		}

		
	}
	
	public Scan(Operator o) throws Exception {
		super(o);
		Scan sc = (Scan) o;
		
		this.filterExpression = new String(sc.filterExpression);
		this.srcTable = new String(sc.srcTable);
		this.tableAlias = new String(sc.tableAlias);
		
		this.filterSet = new HashSet<>();
		for (String s : sc.filterSet) {
			this.filterSet.add(new String(s));
		}
		this.table = new Table();
		try {
			this.table.setName(new String(sc.table.getName()));
			this.table.setSchemaName(new String(sc.table.getSchemaName()));
			this.table.setAlias(sc.table.getAlias());
			this.table.setASTNode(sc.table.getASTNode());
			this.table.setDatabase(sc.table.getDatabase());
			this.table.setPivot(sc.table.getPivot());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Table getTable() {
		return table;
		
	}
	
	@Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {

		if(dstStatement == null) {
			dstStatement = SelectUtils.buildSelectFromTable(table);
		}
		
		if(filterExpression != null && !isPruned) {
			PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
			
			if(ps.getWhere() != null) {
				filterExpression = "";
				for (String s : filterSet) {
					if (filterExpression.equals("")) {
						filterExpression = s;
					} else {
						filterExpression = filterExpression + " AND " + s;
					}
				}
			}
			
			Expression where = 	CCJSqlParserUtil.parseCondExpression(filterExpression);

			ps.setWhere(where);

		}
		

		return dstStatement;

		
	}

	@Override
	public Map<String, ArrayList<String>> getTableLocations(Map<String, ArrayList<String>> locations) {
		Map<String, ArrayList<String>> result = new HashMap<>();
		if (children != null) {
			for (Operator o : children)
				result.putAll(o.getTableLocations(locations));
		}
		String schemaAndName = table.getName();
		if (table.getSchemaName() != null) schemaAndName = table.getSchemaName() + "." +schemaAndName;
		result.put(schemaAndName, locations.get(schemaAndName));
		return result;
	}

}

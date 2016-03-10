package istc.bigdawg.plan.operators;

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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.SelectUtils;

public class Scan extends Operator {
	
//	protected String filterExpressionString = null;
	protected Expression filterExpression = null;
	protected Set<Expression> filterSet;
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
		
		if (parameters.get("Filter") != null) {
			filterExpression = CCJSqlParserUtil.parseCondExpression(parameters.get("Filter"));
			filterSet = new HashSet<Expression>();
			filterSet.add(filterExpression);
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
			filterSet = new HashSet<Expression>();
			filterSet.add(filterExpression);
//			filterExpressionString = SQLUtilities.parseString(parameters.get("Filter"));
//			filterSet = new HashSet<String>();
//			filterSet.add(filterExpressionString);
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

		if (sc.filterSet != null) {
			this.filterSet = new HashSet<>();
			for (Expression s : sc.filterSet) {
				this.filterSet.add(CCJSqlParserUtil.parseCondExpression(s.toString()));
			}
		}
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
	public Select generateSQLStringDestOnly(Select dstStatement, boolean stopAtJoin) throws Exception {

		if(dstStatement == null) {
			dstStatement = SelectUtils.buildSelectFromTable(table);
		}
		
		if (filterExpression != null && !isPruned()) { // used to have a !isPruned;
			PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
			
			Expression e = null; 
			if(ps.getWhere() != null) {
				for (Expression s : filterSet) {
					if (e == null) {
						e = s;
					} else {
						e = new AndExpression(e, s); 
					}
				}
			} else {
				e = filterExpression;
			}
			
			try {
				Expression where = 	e;
				ps.setWhere(where);
			} catch (Exception ex) {
				ex.printStackTrace();
				System.out.println("exception: "+filterExpression.toString());
			}
		}
		return dstStatement;

		
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

}

package istc.bigdawg.plan.operators;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.schema.SQLTable;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SeqScan extends Scan {

	
	
//	private SQLDatabaseSingleton catalog;
	
	
	
	// this is another difference from regular sql processing where the inclination is to keep the rows whole until otherwise needed
	SeqScan(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		// match output to base relation

		String schemaAndName = parameters.get("Schema");
		if (schemaAndName == null || schemaAndName.equals("public")) schemaAndName = super.srcTable;
		else schemaAndName = schemaAndName + "." + super.srcTable;
		
		this.dataObjects.add(schemaAndName);
		
		int dbid = CatalogViewer.getDbsOfObject(schemaAndName).get(0); // TODO FIX THIS; MAKE SURE THE RIGHT DATABASE (SQL) IS REFERENCED
		
		
		
		Connection con = PostgreSQLHandler.getConnection(((PostgreSQLConnectionInfo)PostgreSQLHandler.generateConnectionInfo(dbid)));
		
		CreateTable create = (CreateTable) CCJSqlParserUtil.parse(PostgreSQLHandler.getCreateTable(con, schemaAndName));
		SQLTable baseTable = new SQLTable(create); 
		
		

		
		for(int i = 0; i < output.size(); ++i) {
			
			String expr = output.get(i); // fully qualified name
			SQLOutItem out = new SQLOutItem(expr, baseTable.getAttributes(), supplement);
			
			SQLAttribute sa =  out.getAttribute();
			String alias = sa.getName();
			
			outSchema.put(alias, sa);
			
		}
		
	}
		
	public SeqScan(Operator o) throws Exception {
		super(o);
//		this.catalog = SQLDatabaseSingleton.getInstance();
	}

	
	
	public String toString() {
		return "Sequential scan over " + srcTable + " Filter: " + filterExpression;
	}
	
	
	
	public String printPlan(int recursionLevel) {

		String planStr =  "SeqScan(" + srcTable + ", " + filterExpression+ ")";
		return planStr;
	}
	
};
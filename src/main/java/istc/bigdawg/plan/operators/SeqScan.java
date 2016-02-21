package istc.bigdawg.plan.operators;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.plan.extract.CommonOutItem;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.schema.DataObject;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SeqScan extends Scan {

	
	
//	private SQLDatabaseSingleton catalog;
	private String operatorName = null;
	
	
	// this is another difference from regular sql processing where the inclination is to keep the rows whole until otherwise needed
	public SeqScan (Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		// match output to base relation

		String schemaAndName = parameters.get("Schema");
		if (schemaAndName == null || schemaAndName.equals("public")) schemaAndName = super.srcTable;
		else schemaAndName = schemaAndName + "." + super.srcTable;
		
		this.dataObjects.add(schemaAndName);
		
		int dbid = CatalogViewer.getDbsOfObject(schemaAndName).get(0); // TODO FIX THIS; MAKE SURE THE RIGHT DATABASE (SQL) IS REFERENCED
		
		
		
		Connection con = PostgreSQLHandler.getConnection(((PostgreSQLConnectionInfo)PostgreSQLHandler.generateConnectionInfo(dbid)));
		
		CreateTable create = (CreateTable) CCJSqlParserUtil.parse(PostgreSQLHandler.getCreateTable(con, schemaAndName));
		DataObject baseTable = new DataObject(create); 
		
		for(int i = 0; i < output.size(); ++i) {
			
			String expr = output.get(i); // fully qualified name
			SQLOutItem out = new SQLOutItem(expr, baseTable.getAttributes(), supplement);
			
			SQLAttribute sa =  out.getAttribute();
			String alias = sa.getName();
			
			outSchema.put(alias, sa);
			
		}
		
		operatorName = "project";
	}
	
	// for AFL
	public SeqScan (Map<String, String> parameters, SciDBArray output, Operator child) throws Exception  {
		super(parameters, output, child);
		
		operatorName = parameters.get("OperatorName");
		
		// match output to base relation

//		String schemaAndName = parameters.get("Schema");
//		if (schemaAndName == null || schemaAndName.equals("public")) schemaAndName = super.srcTable;
//		else schemaAndName = schemaAndName + "." + super.srcTable;
//		
//		this.dataObjects.add(schemaAndName);
//		
//		int dbid = CatalogViewer.getDbsOfObject(schemaAndName).get(0); // TODO FIX THIS; MAKE SURE THE RIGHT DATABASE (SQL) IS REFERENCED
//		
//		
//		
//		Connection con = SciDBHandler.getConnection(((SciDBConnectionInfo)SciDBHandler.generateConnectionInfo(dbid)));
//		
//		CreateTable create = (CreateTable) CCJSqlParserUtil.parse(PostgreSQLHandler.getCreateTable(con, schemaAndName));
//		DataObject baseTable = new DataObject(output); 
		
		for (String expr : output.getAttributes().keySet()) {
			CommonOutItem out = new CommonOutItem(expr, output.getAttributes().get(expr), null);
			
			DataObjectAttribute sa =  out.getAttribute();
			String alias = sa.getName();
			
			outSchema.put(alias, sa);
			
		}
		
	}
		
	public SeqScan(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		this.operatorName = ((SeqScan)o).operatorName;
	}

	
	
	public String toString() {
		return "Sequential scan over " + srcTable + " Filter: " + filterExpression;
	}
	
	
	@Override
	public String printPlan(int recursionLevel) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append(operatorName).append('(');
		
		boolean ped = (!this.getChildren().isEmpty()) && this.getChildren().get(0).isPruned();
		
		if (ped)
			sb.append(this.getChildren().get(0).getPruneToken());
		else 
			sb.append(srcTable);
		switch (operatorName) {
		case "project":
			for (String s : outSchema.keySet()){
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
		sb.append(')');
		return sb.toString();
	}
	
};
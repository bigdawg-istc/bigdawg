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
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.schema.DataObject;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SeqScan extends Scan {

	private static int defaultSchemaServerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	
	private String operatorName = null;
	
	
	// this is another difference from regular sql processing where the inclination is to keep the rows whole until otherwise needed
	public SeqScan (Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		// match output to base relation

		String schemaAndName = parameters.get("Schema");
		if (schemaAndName == null || schemaAndName.equals("public")) schemaAndName = super.srcTable;
		else schemaAndName = schemaAndName + "." + super.srcTable;
		
		this.dataObjects.add(schemaAndName);
		
		int dbid;

//		System.out.println(schemaAndName);
		
		if (super.srcTable.toLowerCase().startsWith("bigdawgtag_")) {
			dbid = defaultSchemaServerDBID;
		} else 
			dbid = CatalogViewer.getDbsOfObject(schemaAndName, "postgres").get(0);
		
		
		
		Connection con = PostgreSQLHandler.getConnection(((PostgreSQLConnectionInfo)PostgreSQLHandler.generateConnectionInfo(dbid)));
		
		String createTableString = PostgreSQLHandler.getCreateTable(con, schemaAndName).replaceAll("\\scharacter[\\(]", " char(");
		CreateTable create = null;
		try {
		create = (CreateTable) CCJSqlParserUtil.parse(createTableString);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		
		DataObject baseTable = new DataObject(create); 
		
		for(int i = 0; i < output.size(); ++i) {
			
			String expr = output.get(i); // fully qualified name
			SQLOutItem out = new SQLOutItem(expr, baseTable.getAttributes(), supplement);
			
			SQLAttribute sa =  out.getAttribute();
			String alias = sa.getName();
			
			outSchema.put(alias, sa);
			
		}
		
		if (filterExpression != null && (!filterExpression.equals("")))
			operatorName = "filter";
		else if (children.size() != 0)
			operatorName = "project";
		else 
			operatorName = "scan";
		
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
		
		// attributes
		for (String expr : output.getAttributes().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getAttributes().get(expr), false, null);
			
			DataObjectAttribute sa =  out.getAttribute();
			String alias = sa.getName();
			
			outSchema.put(alias, sa);
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getDimensions().get(expr), true, null);
			
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			
			outSchema.put(attrName, attr);
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
	public String generateAFLString(int recursionLevel) throws Exception {
		StringBuilder sb = new StringBuilder();
		if (!(operatorName.equals("scan") && recursionLevel > 0))
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
		if (!(operatorName.equals("scan") && recursionLevel > 0))
			sb.append(')');
		return sb.toString();
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot){
		
		if (isPruned() && (!isRoot)) {
			return "{PRUNED}";
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		if (children.isEmpty() && operatorName.equals("scan")){
			// it is a scan
			sb.append(this.srcTable);
		} else if (children.isEmpty()) {
			sb.append(operatorName).append('{').append(this.srcTable).append('}');
		} else {
			// filter, project
			sb.append(operatorName).append(children.get(0).getTreeRepresentation(false));
		}
		
		sb.append('}');
		
		return sb.toString();
	}
};
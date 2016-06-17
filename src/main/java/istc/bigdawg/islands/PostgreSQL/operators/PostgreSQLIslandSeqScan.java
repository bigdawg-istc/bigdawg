package istc.bigdawg.islands.PostgreSQL.operators;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLOutItem;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.schema.DataObject;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class PostgreSQLIslandSeqScan extends PostgreSQLIslandScan implements SeqScan {

	private static int defaultSchemaServerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	
	// this is another difference from regular sql processing where the inclination is to keep the rows whole until otherwise needed
	public PostgreSQLIslandSeqScan (Map<String, String> parameters, List<String> output, PostgreSQLIslandOperator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		// match output to base relation

		String schemaAndName = parameters.get("Schema");
		if (schemaAndName == null || schemaAndName.equals("public")) schemaAndName = super.getSrcTable();
		else schemaAndName = schemaAndName + "." + super.getSrcTable();
		
		this.dataObjects.add(schemaAndName);
		
		int dbid;

		if (super.getSrcTable().toLowerCase().startsWith("bigdawgtag_")) {
			dbid = defaultSchemaServerDBID;
		} else 
			dbid = CatalogViewer.getDbsOfObject(schemaAndName, "postgres").get(0);
		
		
		Connection con = PostgreSQLHandler.getConnection((PostgreSQLConnectionInfo)CatalogViewer.getConnectionInfo(dbid));
		
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
		
		if (getFilterExpression() != null && (!getFilterExpression().equals("")))
			setOperatorName("filter");
		else if (children.size() != 0)
			setOperatorName("project");
		else 
			setOperatorName("scan");
		
	}
	
	public PostgreSQLIslandSeqScan(PostgreSQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		this.setOperatorName(((PostgreSQLIslandSeqScan)o).getOperatorName());
	}

	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	
	public String toString() {
		return "SeqScan " + getSrcTable() + " subject to (" + getFilterExpression()+")";
	}
	
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		
		if (isPruned() && (!isRoot)) {
			return "{PRUNED}";
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		if (children.isEmpty() && getOperatorName().equals("scan")){
			// it is a scan
			sb.append(this.getSrcTable());
		} else if (children.isEmpty()) {
			sb.append(getOperatorName()).append('{').append(this.getSrcTable()).append('}');
		} else {
			// filter, project
			sb.append(getOperatorName()).append(children.get(0).getTreeRepresentation(false));
		}
//		if (filterExpression != null) sb.append(SQLExpressionUtils.parseCondForTree(filterExpression));
		sb.append('}');
		
		return sb.toString();
	}

	@Override
	public String getFullyQualifiedName() {
		return getTable().getFullyQualifiedName();
	}
};
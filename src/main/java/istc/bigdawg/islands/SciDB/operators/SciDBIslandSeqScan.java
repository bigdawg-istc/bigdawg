package istc.bigdawg.islands.SciDB.operators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.islands.CommonOutItem;
import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.SeqScan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class SciDBIslandSeqScan extends SciDBIslandScan implements SeqScan {

//	private static int defaultSchemaServerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	
//	// this is another difference from regular sql processing where the inclination is to keep the rows whole until otherwise needed
//	public SciDBIslandSeqScan (Map<String, String> parameters, List<String> output, SciDBIslandOperator child, SQLTableExpression supplement) throws Exception  {
//		super(parameters, output, child, supplement);
//		
//		
//		// match output to base relation
//
//		String schemaAndName = parameters.get("Schema");
//		if (schemaAndName == null || schemaAndName.equals("public")) schemaAndName = super.getSrcTable();
//		else schemaAndName = schemaAndName + "." + super.getSrcTable();
//		
//		this.dataObjects.add(schemaAndName);
//		
//		int dbid;
//
//		if (super.getSrcTable().toLowerCase().startsWith("bigdawgtag_")) {
//			dbid = defaultSchemaServerDBID;
//		} else 
//			dbid = CatalogViewer.getDbsOfObject(schemaAndName, "postgres").get(0);
//		
//		
////		Connection con = PostgreSQLHandler.getConnection(((PostgreSQLConnectionInfo)PostgreSQLHandler.generateConnectionInfo(dbid)));
//		Connection con = PostgreSQLHandler.getConnection(CatalogViewer.getConnection(dbid));
//		
//		String createTableString = PostgreSQLHandler.getCreateTable(con, schemaAndName).replaceAll("\\scharacter[\\(]", " char(");
//		CreateTable create = null;
//		try {
//		create = (CreateTable) CCJSqlParserUtil.parse(createTableString);
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw e;
//		}
//		
//		
//		DataObject baseTable = new DataObject(create); 
//		
//		for(int i = 0; i < output.size(); ++i) {
//			
//			String expr = output.get(i); // fully qualified name
//			
//			SQLOutItem out = new SQLOutItem(expr, baseTable.getAttributes(), supplement);
//			
//			SQLAttribute sa =  out.getAttribute();
//			String alias = sa.getName();
//			
//			outSchema.put(alias, sa);
//			
//		}
//		
//		if (getFilterExpression() != null && (!getFilterExpression().equals("")))
//			setOperatorName("filter");
//		else if (children.size() != 0)
//			setOperatorName("project");
//		else 
//			setOperatorName("scan");
//		
//	}
	
	// for AFL
	public SciDBIslandSeqScan (Map<String, String> parameters, SciDBArray output, Operator child) throws Exception  {
		super(parameters, output, child);
		
		setOperatorName(parameters.get("OperatorName"));
		
		Map<String, String> applyAttributes = new HashMap<>();
		if (parameters.get("Apply-Attributes") != null) {
			List<String> applyAttributesList = Arrays.asList(parameters.get("Apply-Attributes").split("@@@@"));
			for (String s : applyAttributesList) {
				String[] sSplit = s.split(" @AS@ ");
				applyAttributes.put(sSplit[1], sSplit[0]);
			}
		}
		
		// attributes
		for (String expr : output.getAttributes().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getAttributes().get(expr), false, null);
			
			DataObjectAttribute attr = out.getAttribute();
			String alias = attr.getName();
			if (!applyAttributes.isEmpty() && applyAttributes.get(expr) != null) attr.setExpression(applyAttributes.get(expr));
			else attr.setExpression(expr);
			
			outSchema.put(alias, attr);
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getDimensions().get(expr), true, null);
			
			DataObjectAttribute dim = out.getAttribute();
			String attrName = dim.getFullyQualifiedName();		
			

			Column e = (Column) CCJSqlParserUtil.parseExpression(expr);
			String arrayName = output.getDimensionMembership().get(expr);
			if (arrayName != null) {
				e.setTable(new Table(Arrays.asList(arrayName.split(", ")).get(0)));
			}
			
			outSchema.put(attrName, dim);
		}
		
	}
		
	public SciDBIslandSeqScan(SciDBIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		this.setOperatorName(((SciDBIslandSeqScan)o).getOperatorName());
	}

	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	
	public String toString() {
		return "SeqScan " + getSourceTableName() + " subject to (" + getFilterExpression()+")";
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
			sb.append(this.getSourceTableName());
		} else if (children.isEmpty()) {
			sb.append(getOperatorName()).append('{').append(this.getSourceTableName()).append('}');
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
		return this.getSourceTableName();
	}
};
package istc.bigdawg.islands.PostgreSQL.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLOutItem;
import istc.bigdawg.islands.PostgreSQL.SQLQueryPlan;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.PostgreSQL.utils.SQLAttribute;
import istc.bigdawg.islands.operators.CommonTableExpressionScan;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.WithItem;

public class PostgreSQLIslandCommonTableExpressionScan extends PostgreSQLIslandScan implements CommonTableExpressionScan {

	private String cteName;
	private WithItem with;
	private PostgreSQLIslandOperator sourceStatement;
	
	
	PostgreSQLIslandCommonTableExpressionScan(Map<String, String> parameters, List<String> output, PostgreSQLIslandOperator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		setSourceTableName(parameters.get("CTE-Name"));
		setWith(plan.getWithItem(getSourceTableName()));
		
		this.dataObjects.add(getSourceTableName());
		
		// match output to base relation
		Map<String, DataObjectAttribute> cteSchema = new HashMap<String, DataObjectAttribute>();
		// insert cte alias for schema resolution
		// delete everything before the first dot and replace it with the tableAlias
		sourceStatement = (PostgreSQLIslandOperator) plan.getPlanRoot(getSourceTableName());
		
		Iterator<Map.Entry<String, DataObjectAttribute>  > schemaItr = sourceStatement.outSchema.entrySet().iterator();

		while(schemaItr.hasNext()) {
			Map.Entry<String, DataObjectAttribute> pair = schemaItr.next();
			String name = pair.getKey();
			String[] names = name.split("\\.");
			
			if(names.length == 2) {
				name = getTableAlias() + "." + names[1];
			}
			cteSchema.put(name, pair.getValue());
					
		}
		
		
		for(int i = 0; i < output.size(); ++i) {
			
			String expr = output.get(i); // fully qualified name
			
			SQLOutItem out = new SQLOutItem(expr, cteSchema, supplement);
			SQLAttribute sa =  out.getAttribute();
			String alias = sa.getName(); // attr alias
			
			sa.setName(alias);
			outSchema.put(alias, sa);
			
		}

	}

	
	public PostgreSQLIslandCommonTableExpressionScan(PostgreSQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		PostgreSQLIslandCommonTableExpressionScan c = (PostgreSQLIslandCommonTableExpressionScan) o;
		this.setSourceTableName(new String(c.getSourceTableName()));
		
		PostgreSQLIslandOperator s = (PostgreSQLIslandOperator) c.sourceStatement.duplicate(true);
		
		if (s instanceof PostgreSQLIslandJoin) {
			this.sourceStatement = new PostgreSQLIslandJoin(s, addChild);
		} else if (s instanceof PostgreSQLIslandSeqScan) {
			this.sourceStatement = new PostgreSQLIslandSeqScan(s, addChild);
		} else if (s instanceof PostgreSQLIslandCommonTableExpressionScan) {
			this.sourceStatement = new PostgreSQLIslandCommonTableExpressionScan(s, addChild);
		} else if (s instanceof PostgreSQLIslandSort) {
			this.sourceStatement = new PostgreSQLIslandSort(s, addChild);
		} else {
			if (s instanceof PostgreSQLIslandAggregate) {
			} else if (s instanceof PostgreSQLIslandDistinct) {
			} else if (s instanceof PostgreSQLIslandWindowAggregate) {
			} else {
				throw new Exception("Unknown Operator from Operator Copy: "+s.getClass().toString());
			}
			throw new Exception("Unsupported Operator Copy: "+s.getClass().toString());
		}
		
		this.setWith(new WithItem());
		try {
			this.getWith().setName(new String(c.getWith().getName()));
			this.getWith().setRecursive(c.getWith().isRecursive());
			this.getWith().setSelectBody(c.getWith().getSelectBody());
			this.getWith().setWithItemList(c.getWith().getWithItemList());
		} catch (Exception e) {
			e.printStackTrace();
		}
//		private String cteName;
//		private WithItem with;
//		private Operator sourceStatement;
	};

	
	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	public String toString() {
		return "CTE scan over " + getSourceTableName() + " Filter: " + getFilterExpression();
	}
	
	public PostgreSQLIslandOperator getSourceStatement() {
		return sourceStatement;
	}
	
//	@Override
//	public String generateAFLString(int recursionLevel) {
//		String planStr =  "CTE_Scan(" + getCteName();
//		if(getFilterExpression() != null) {
//			planStr += ", " + getFilterExpression();
//		}
//		
//		//planStr += children.get(0).printPlan(recursionLevel + 1);
//		planStr += ")";
//		return planStr;
//	}
	
	@Override
	public Map<String, List<String>> getTableLocations(Map<String, List<String>> locations) {

		// the assumption here is that there is no Nested Query
		// since the order of execution is unforeseeable, all will become place holder
		
		Map<String, List<String>> result = new HashMap<>();
		Set<String> sas = new HashSet<String>();
		ArrayList<String> outs = new ArrayList<String> ();
		
		PlainSelect ps = (PlainSelect) getWith().getSelectBody();
		
		sas.add(((Table)ps.getFromItem()).getName());
		
		if (ps.getJoins() != null) {
			for (net.sf.jsqlparser.statement.select.Join j : ps.getJoins()) {
				sas.add( ((Table) j.getRightItem()).getName());
			}
		}
		
		outs.addAll(sas);
		result.put(getSourceTableName(), outs);
		
		locations.putAll(sourceStatement.getTableLocations(locations));
		locations.put(getSourceTableName(), outs);
		
		return result;
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		return "{with{"+this.getSourceTableName()+"}"+this.sourceStatement.getTreeRepresentation(false)+"}";
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		return sourceStatement.getObjectToExpressionMappingForSignature();
	}
	
	@Override
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws Exception {
		
		if (entry.containsKey(getSourceTableName()))
			entry.remove(getSourceTableName());
		((PostgreSQLIslandOperator) sourceStatement).removeCTEEntriesFromObjectToExpressionMapping(entry);
	}


//	public String getCteName() {
//		return cteName;
//	}
//
//
//	public void setCteName(String cteName) {
//		this.cteName = cteName;
//	}


	public WithItem getWith() {
		return with;
	}


	public void setWith(WithItem with) {
		this.with = with;
	}


	@Override
	public String getSourceTableName() {
		return cteName;
	}


	@Override
	public void setSourceTableName(String srcTableName) {
		this.cteName = srcTableName;
	}

};
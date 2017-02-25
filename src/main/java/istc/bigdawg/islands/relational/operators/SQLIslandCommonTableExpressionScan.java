package istc.bigdawg.islands.relational.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.operators.CommonTableExpressionScan;
import istc.bigdawg.islands.relational.SQLOutItemResolver;
import istc.bigdawg.islands.relational.SQLQueryPlan;
import istc.bigdawg.islands.relational.SQLTableExpression;
import istc.bigdawg.islands.relational.utils.SQLAttribute;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.WithItem;

public class SQLIslandCommonTableExpressionScan extends SQLIslandScan implements CommonTableExpressionScan {

	private String cteName;
	private WithItem with;
	private SQLIslandOperator sourceStatement;
	
	
	SQLIslandCommonTableExpressionScan(Map<String, String> parameters, List<String> output, SQLIslandOperator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		setSourceTableName(parameters.get("CTE-Name"));
		setWith(plan.getWithItem(getSourceTableName()));
		
		this.dataObjects.add(getSourceTableName());
		
		// match output to base relation
		Map<String, DataObjectAttribute> cteSchema = new HashMap<String, DataObjectAttribute>();
		// insert cte alias for schema resolution
		// delete everything before the first dot and replace it with the tableAlias
		sourceStatement = (SQLIslandOperator) plan.getPlanRoot(getSourceTableName());
		
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
			
			SQLOutItemResolver out = new SQLOutItemResolver(expr, cteSchema, supplement);
			SQLAttribute sa =  out.getAttribute();
			String alias = sa.getName(); // attr alias
			
			sa.setName(alias);
			outSchema.put(alias, sa);
			
		}

	}

	
	public SQLIslandCommonTableExpressionScan(SQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		SQLIslandCommonTableExpressionScan c = (SQLIslandCommonTableExpressionScan) o;
		this.setSourceTableName(new String(c.getSourceTableName()));
		
		SQLIslandOperator s = (SQLIslandOperator) c.sourceStatement.duplicate(true);
		
		if (s instanceof SQLIslandJoin) {
			this.sourceStatement = new SQLIslandJoin(s, addChild);
		} else if (s instanceof SQLIslandSeqScan) {
			this.sourceStatement = new SQLIslandSeqScan(s, addChild);
		} else if (s instanceof SQLIslandCommonTableExpressionScan) {
			this.sourceStatement = new SQLIslandCommonTableExpressionScan(s, addChild);
		} else if (s instanceof SQLIslandSort) {
			this.sourceStatement = new SQLIslandSort(s, addChild);
		} else {
			if (s instanceof SQLIslandAggregate) {
			} else if (s instanceof SQLIslandDistinct) {
			} else if (s instanceof SQLIslandWindowAggregate) {
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
	};

	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	public String toString() {
		return "CTE scan over " + getSourceTableName() + " Filter: " + getFilterExpression();
	}
	
	public SQLIslandOperator getSourceStatement() {
		return sourceStatement;
	}
	
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
		((SQLIslandOperator) sourceStatement).removeCTEEntriesFromObjectToExpressionMapping(entry);
	}


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
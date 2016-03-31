package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;

public class CommonSQLTableExpressionScan extends Scan {

	private String cteName;
	private WithItem with;
	private Operator sourceStatement;
	
	
	CommonSQLTableExpressionScan(Map<String, String> parameters, List<String> output, Operator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		cteName = parameters.get("CTE-Name");
		with = plan.getWithItem(cteName);
		
		this.dataObjects.add(cteName);
		
		// match output to base relation
		Map<String, DataObjectAttribute> cteSchema = new HashMap<String, DataObjectAttribute>();
		// insert cte alias for schema resolution
		// delete everything before the first dot and replace it with the tableAlias
		sourceStatement = plan.getPlanRoot(cteName);
		
		Iterator<Map.Entry<String, DataObjectAttribute>  > schemaItr = sourceStatement.outSchema.entrySet().iterator();

		while(schemaItr.hasNext()) {
			Map.Entry<String, DataObjectAttribute> pair = schemaItr.next();
			String name = pair.getKey();
			String[] names = name.split("\\.");
			
			if(names.length == 2) {
				name = tableAlias + "." + names[1];
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

	
	public CommonSQLTableExpressionScan(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		CommonSQLTableExpressionScan c = (CommonSQLTableExpressionScan) o;
		this.cteName = new String(c.cteName);
		
		Operator s = c.sourceStatement.duplicate(true);
		
		if (s instanceof Join) {
			this.sourceStatement = new Join(s, addChild);
		} else if (s instanceof SeqScan) {
			this.sourceStatement = new SeqScan(s, addChild);
		} else if (s instanceof CommonSQLTableExpressionScan) {
			this.sourceStatement = new CommonSQLTableExpressionScan(s, addChild);
		} else if (s instanceof Sort) {
			this.sourceStatement = new Sort(s, addChild);
		} else {
			if (s instanceof Aggregate) {
			} else if (s instanceof Distinct) {
			} else if (s instanceof WindowAggregate) {
			} else {
				throw new Exception("Unknown Operator from Operator Copy: "+s.getClass().toString());
			}
			throw new Exception("Unsupported Operator Copy: "+s.getClass().toString());
		}
		
		this.with = new WithItem();
		try {
			this.with.setName(new String(c.with.getName()));
			this.with.setRecursive(c.with.isRecursive());
			this.with.setSelectBody(c.with.getSelectBody());
			this.with.setWithItemList(c.with.getWithItemList());
		} catch (Exception e) {
			e.printStackTrace();
		}
//		private String cteName;
//		private WithItem with;
//		private Operator sourceStatement;
	};

	@Override
	public Select generateSQLStringDestOnly(Select dstStatement, Boolean stopAtJoin, Set<String> allowedScans) throws Exception {
		dstStatement = super.generateSQLStringDestOnly(dstStatement, stopAtJoin, allowedScans);
		 
		List<WithItem> withs = dstStatement.getWithItemsList();
		
		boolean found = false;
		if(withs != null && !withs.isEmpty()) {
			for(WithItem w : withs) {
				if(w.getName().equals(cteName)) {
					found = true;
				}
			}
		}
		
		
		// insert WithItem at the beginning of list
		if(!found) {
			List<WithItem> dstWiths = new ArrayList<WithItem>();
			
			if(dstStatement.getWithItemsList() != null && !(dstStatement.getWithItemsList().isEmpty())) {
				dstWiths.addAll(dstStatement.getWithItemsList());
			}
			dstWiths.add(with);

			
			dstStatement.setWithItemsList(dstWiths);
			// recurse if child references any additional CTEs
			// create new dst statement for child and grab its select body
			
			Select dstPrime = sourceStatement.generateSQLStringDestOnly(null, stopAtJoin, new HashSet<>());

			List<WithItem> dstWithsPrime = dstPrime.getWithItemsList();
			if(dstWithsPrime != null) {
				for(int i = 0; i < dstWithsPrime.size(); ++i) {
					if(dstWiths.contains(dstWithsPrime.get(i))) {
						dstWithsPrime.remove(i);
						i = 0; // restart
					}
				}
				if(!dstWithsPrime.isEmpty()) {
					dstWithsPrime.addAll(dstWiths);
					dstStatement.setWithItemsList(dstWithsPrime);
				}
			} // end "have child cte" check
		} // end "adding cte" check

		return dstStatement;
	
	}
	
	
	public String toString() {
		return "CTE scan over " + cteName + " Filter: " + filterExpression;
	}
	
	public Operator getSourceStatement() {
		return sourceStatement;
	}
	
	@Override
	public String generateAFLString(int recursionLevel) {
		String planStr =  "CTE_Scan(" + cteName;
		if(filterExpression != null) {
			planStr += ", " + filterExpression;
		}
		
		//planStr += children.get(0).printPlan(recursionLevel + 1);
		planStr += ")";
		return planStr;
	}
	
	@Override
	public Map<String, List<String>> getTableLocations(Map<String, List<String>> locations) {

		// the assumption here is that there is no Nested Query
		// since the order of execution is unforeseeable, all will become place holder
		
		Map<String, List<String>> result = new HashMap<>();
		Set<String> sas = new HashSet<String>();
		ArrayList<String> outs = new ArrayList<String> ();
		
		PlainSelect ps = (PlainSelect) with.getSelectBody();
		
		sas.add(((Table)ps.getFromItem()).getName());
		
		if (ps.getJoins() != null) {
			for (net.sf.jsqlparser.statement.select.Join j : ps.getJoins()) {
				sas.add( ((Table) j.getRightItem()).getName());
			}
		}
		
		outs.addAll(sas);
		result.put(cteName, outs);
		
		locations.putAll(sourceStatement.getTableLocations(locations));
		locations.put(cteName, outs);
		
		return result;
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		return "{with{"+this.cteName+"}"+this.sourceStatement.getTreeRepresentation(false)+"}";
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		return sourceStatement.getObjectToExpressionMappingForSignature();
	}
	
	@Override
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) {
		
		if (entry.containsKey(cteName))
			entry.remove(cteName);
		sourceStatement.removeCTEEntriesFromObjectToExpressionMapping(entry);
	}
};
package teddy.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.WithItem;
import teddy.bigdawg.extract.logical.SQLTableExpression;
import teddy.bigdawg.plan.SQLQueryPlan;
import teddy.bigdawg.plan.extract.SQLOutItem;
import teddy.bigdawg.schema.SQLAttribute;
import teddy.bigdawg.plan.operators.Operator;

public class CommonSQLTableExpressionScan extends Scan {

	private String cteName;
	private WithItem with;
	private Operator sourceStatement;
	
	
	CommonSQLTableExpressionScan(Map<String, String> parameters, List<String> output, Operator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		

		cteName = parameters.get("CTE-Name");
		with = plan.getWithItem(cteName);
		
		
		// match output to base relation
		Map<String, SQLAttribute> cteSchema = new HashMap<String, SQLAttribute>();
		// insert cte alias for schema resolution
		// delete everything before the first dot and replace it with the tableAlias
		sourceStatement = plan.getPlanRoot(cteName);
		
//		secureCoordination = sourceStatement.secureCoordination;
		
		Iterator<Map.Entry<String, SQLAttribute>  > schemaItr = sourceStatement.outSchema.entrySet().iterator();

		while(schemaItr.hasNext()) {
			Map.Entry<String, SQLAttribute> pair = (Map.Entry<String, SQLAttribute>) schemaItr.next();
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


	@Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {
		dstStatement = super.generatePlaintext(srcStatement, dstStatement);
		 
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
			
			Select dstPrime = sourceStatement.generatePlaintext(srcStatement, null);

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
	
	
	@Override
	public List<SQLAttribute> getSliceKey()  throws JSQLParserException {
		return null;
	}
	
	public String toString() {
		return "CTE scan over " + cteName + " Filter: " + filterExpression;
	}
	
	public Operator getSourceStatement() {
		return sourceStatement;
	}
	
	@Override
	public String printPlan(int recursionLevel) {
		String planStr =  "CTE_Scan(" + cteName;
		if(filterExpression != null) {
			planStr += ", " + filterExpression;
		}
		
		//planStr += children.get(0).printPlan(recursionLevel + 1);
		planStr += ")";
		return planStr;
	}
	
	@Override
	public Map<String, ArrayList<String>> getTableLocations(Map<String, ArrayList<String>> locations) {

		// the assumption here is that there is no Nested Query
		// since the order of execution is unforeseeable, all will become place holder
		
		Map<String, ArrayList<String>> result = new HashMap<>();
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
};
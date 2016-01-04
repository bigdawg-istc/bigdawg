package istc.bigdawg.plan.operators;

import java.util.List;
import java.util.Map;

import istc.bigdawg.schema.SQLDatabaseSingleton;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.schema.SQLTable;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.extract.SQLOutItem;

public class SeqScan extends Scan {

	
	
	private SQLDatabaseSingleton catalog;
	
	
	
	// this is another difference from regular sql processing where the inclination is to keep the rows whole until otherwise needed
	SeqScan(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		catalog = SQLDatabaseSingleton.getInstance();
		
		
		// match output to base relation
		SQLTable baseTable = new SQLTable(catalog.getDatabase().getTable(super.srcTable));
		
		
		for(int i = 0; i < output.size(); ++i) {
			
			String expr = output.get(i); // fully qualified name
			SQLOutItem out = new SQLOutItem(expr, baseTable.getAttributes(), supplement);
			
			SQLAttribute sa =  out.getAttribute();
			String alias = sa.getName();
			
			outSchema.put(alias, sa);
			
		}
		
	}
		


	
	
	public String toString() {
		return "Sequential scan over " + srcTable + " Filter: " + filterExpression;
	}
	
	
	
	public String printPlan(int recursionLevel) {

		String planStr =  "SeqScan(" + srcTable + ", " + filterExpression+ ")";
		return planStr;
	}
	
};
package teddy.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



import org.apache.jcp.xml.dsig.internal.dom.Utils;
import teddy.bigdawg.schema.SQLAttribute;
import teddy.bigdawg.extract.logical.SQLTableExpression;
import teddy.bigdawg.plan.SQLQueryPlan;
import teddy.bigdawg.plan.extract.SQLOutItem;
import teddy.bigdawg.util.SQLUtilities;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;


// TODO: expressions on aggregates - e.g., COUNT(*) / COUNT(v > 5)

public class Aggregate extends Operator {

	// can address complex expressions by adding a step after aggregate
	// create a list of aggregations to perform
	
	public enum AggregateType { MIN, MAX, COUNT, COUNT_DISTINCT, AVG };
	private List<SQLAttribute> groupBy;
	private List<String> aggregateExpressions; // e.g., COUNT(SOMETHING)
	private List<AggregateType>  aggregates; 
	private List<String> aggregateAliases; 
	private List<Function> parsedAggregates;
	private List<Expression> parsedGroupBys;
	private String aggregateFilter; // HAVING clause
	
	
	// TODO: write ObliVM aggregate as a for loop over values, 
	// maintain state once per aggregate added
	// apply any expressions down the line in the final selection
	
	Aggregate(Map<String, String> parameters, List<String> output, Operator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		
		isBlocking = true;
//		isSplittable = true;
		isLocal = false;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		aggregateAliases = new ArrayList<String>(); 
		groupBy = new ArrayList<SQLAttribute>();
	
		parsedAggregates = new ArrayList<Function>();
		parsedGroupBys = supplement.getGroupBy();
		aggregateFilter = parameters.get("Filter");
		if(aggregateFilter != null) {
			aggregateFilter = Utils.parseIdFromSameDocumentURI(aggregateFilter); // HAVING clause
		}
		
//		secureCoordination = children.get(0).secureCoordination;
		
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);

			
			SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
			SQLAttribute attr = out.getAttribute();
			String attrName = attr.getName();
			
			
			outSchema.put(attrName, attr);
			
			
			// e.g., sum(y) / count(x)
			if(out.hasAggregate()) {
				List<Function> parsedAggregates = out.getAggregates();
				for(int j = 0; j < parsedAggregates.size(); ++j) {
					processFunction(parsedAggregates.get(j), attrName);
				}
				
				
			}
			else {
				groupBy.add(attr);
				/*if(attr.getSecurityPolicy() != Attribute.SecurityPolicy.Public) {
					throw new Exception("Aggregation must only group by public attributes.");
				}*/
			}
			
		}
		


		
		
		

	

	}
	
	void processFunction(Function f, String alias) throws Exception  {
		switch(f.getName()) {
			case "min":
				aggregates.add(AggregateType.MIN);
				break;
			case "max":
				aggregates.add(AggregateType.MAX);
				break;
			case "avg":
				aggregates.add(AggregateType.AVG);
				break;
			case "count":
				if(f.isDistinct())  {
						aggregates.add(AggregateType.COUNT_DISTINCT); }
				else {
					aggregates.add(AggregateType.COUNT); }
				break;
			default:
				throw new Exception("Unknown aggregate type " + f.getName());
		}

		if(f.getParameters() != null) {
			String parameter = f.getParameters().toString();
			aggregateExpressions.add(parameter);
			parameter = SQLUtilities.removeOuterParens(parameter);
			// check for secure coordination
//			SQLAttribute attr = children.get(0).outSchema.get(parameter);
			
//			if(attr != null) {
//				updateSecurityPolicy(attr);
//			}
		}
		else {
			aggregateExpressions.add("");
		}
		
		aggregateAliases.add(alias);
			
	}
	
	public Aggregate() {
		isBlocking = true;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		
		
	}

	@Override
	public List<SQLAttribute> getSliceKey()  throws JSQLParserException {
		List<SQLAttribute> sliceKey = new ArrayList<SQLAttribute>();
		for(SQLAttribute a : groupBy) {
//			if(a.getSecurityPolicy().equals(SQLAttribute.SecurityPolicy.Public)) {
				sliceKey.add(a);
//			}
		}
		
		return sliceKey;
	}
	
	public void addAggregate(AggregateType a, String aFilter) {
		aggregates.add(a);
		aggregateExpressions.add(aFilter);
	}
	
	
	@Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {

		dstStatement = children.get(0).generatePlaintext(srcStatement, dstStatement);
				
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		ps.setGroupByColumnReferences(parsedGroupBys);
		
		return dstStatement;
		
	}
	
	@Override
	public String toString() {
		return "Aggregating on " + aggregateExpressions.toString() + " group by " + groupBy + " types " + aggregates.toString();
	}
	
	@Override
	public String printPlan(int recursionLevel) {
		
		String planStr =  "Aggregate(";
		planStr += children.get(0).printPlan(recursionLevel+1);
		planStr +=  ", (";
		
		for(int i = 0; i < aggregates.size(); ++i) {
			planStr += aggregates.get(i) + "(" + aggregateExpressions.get(i)  + ") " + aggregateAliases.get(i);
		}
		planStr += "), ";
		if(groupBy.size() > 0) {
			planStr += groupBy.get(0).getName();
		}
				
		for(int i = 1; i < groupBy.size(); ++i) {
			planStr += ", " + groupBy.get(i).getName();
		}

		planStr +=  ")";
		
		return planStr;
	}

};
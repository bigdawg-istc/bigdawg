package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLOutItem;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Distinct extends Operator {

	
	
	Distinct(Map<String, String> parameters, List<String> output, Operator child, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		isBlocking = true;

//		secureCoordination = child.secureCoordination;
		Map<String, SQLAttribute> srcSchema = child.getOutSchema(); //TODO CHECK THIS? TODO TODO
		for(int i = 0; i < output.size(); ++i) {
			
			String expr = output.get(i); // fully qualified name
			
			SQLOutItem out = new SQLOutItem(expr, srcSchema, supplement);
			SQLAttribute sa =  out.getAttribute();
//			updateSecurityPolicy(sa);
			
			
			String alias = sa.getName(); // attr alias
			
			sa.setName(alias);
			outSchema.put(alias, sa);
			
		}
		

	}

	
	@Override
	public Select generatePlaintext(Select srcStatement, Select dstStatement) throws Exception {
		
		dstStatement = children.get(0).generatePlaintext(srcStatement, dstStatement);
		
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		ps.setDistinct(new net.sf.jsqlparser.statement.select.Distinct());
		
		return dstStatement;
		
	}

	
	@Override
	public List<SQLAttribute> getSliceKey()  throws JSQLParserException {
		List<SQLAttribute> sliceKey = new ArrayList<SQLAttribute>();
		for(SQLAttribute a : outSchema.values()) {
//			if(a.getSecurityPolicy().equals(SQLAttribute.SecurityPolicy.Public)) {
				sliceKey.add(a);
//			}
		}
		
		return sliceKey;
		
	}
	
	public String toString() {
		return "Distinct over " + outSchema;
	}
	
	@Override
	public String printPlan(int recursionLevel) {
		String planStr =  "Distinct(";
		planStr += children.get(0).printPlan(recursionLevel + 1);
		planStr += ")";
		return planStr;
	}
	
};
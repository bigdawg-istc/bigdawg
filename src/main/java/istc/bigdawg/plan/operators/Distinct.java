package istc.bigdawg.plan.operators;

import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Distinct extends Operator {

	
	
	Distinct(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

//		secureCoordination = child.secureCoordination;
		Map<String, DataObjectAttribute> srcSchema = child.getOutSchema(); //TODO CHECK THIS? TODO TODO
		for(int i = 0; i < output.size(); ++i) {
			
			String expr = output.get(i); // fully qualified name
			
			SQLOutItem out = new SQLOutItem(expr, srcSchema, supplement);
			SQLAttribute sa =  out.getAttribute();
			sa.setExpression(rewriteComplextOutItem(sa.getExpressionString()));
//			updateSecurityPolicy(sa);
			
			
			String alias = sa.getName(); // attr alias
			
			sa.setName(alias);
			outSchema.put(alias, sa);
			
		}
		

	}

	
	@Override
	public Select generateSQLStringDestOnly(Select dstStatement, Boolean stopAtJoin, Set<String> allowedScans) throws Exception {
		
		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, stopAtJoin, allowedScans);
		
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		ps.setDistinct(new net.sf.jsqlparser.statement.select.Distinct());
		
		return dstStatement;
		
	}

	
	
	public String toString() {
		return "Distinct over " + outSchema;
	}
	
	@Override
	public String generateAFLString(int recursionLevel) throws Exception {
		String planStr =  "Distinct(";
		planStr += children.get(0).generateAFLString(recursionLevel + 1);
		planStr += ")";
		return planStr;
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		return "{distinct" + this.getChildren().get(0).getTreeRepresentation(isRoot)+"}";
	}
};
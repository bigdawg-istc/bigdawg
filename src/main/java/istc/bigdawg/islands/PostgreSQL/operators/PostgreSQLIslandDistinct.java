package istc.bigdawg.islands.PostgreSQL.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLOutItem;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.schema.SQLAttribute;

public class PostgreSQLIslandDistinct extends PostgreSQLIslandOperator implements Distinct {

	
	
	PostgreSQLIslandDistinct(Map<String, String> parameters, List<String> output, PostgreSQLIslandOperator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		if (children.get(0) instanceof PostgreSQLIslandJoin) {
			outSchema = new LinkedHashMap<>();
			for(int i = 0; i < output.size(); ++i) {
				String expr = output.get(i);
				SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
				SQLAttribute attr = out.getAttribute();
				String attrName = attr.getName();
				outSchema.put(attrName, attr);
			}
		} else {
			outSchema = new LinkedHashMap<>(child.outSchema);
		}
		
	}

	
	public PostgreSQLIslandDistinct(PostgreSQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
	}


	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	public String toString() {
		return "Distinct over " + outSchema;
	}
	
//	@Override
//	public String generateAFLString(int recursionLevel) throws Exception {
//		String planStr =  "Distinct(";
//		planStr += children.get(0).generateAFLString(recursionLevel + 1);
//		planStr += ")";
//		return planStr;
//	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		return "{distinct" + this.getChildren().get(0).getTreeRepresentation(isRoot)+"}";
	}
};
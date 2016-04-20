package istc.bigdawg.plan.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.plan.generators.OperatorVisitor;
import istc.bigdawg.schema.SQLAttribute;

public class Distinct extends Operator {

	
	
	Distinct(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);
		
		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		if (children.get(0) instanceof Join) {
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

	
	public Distinct(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
	}


//	@Override
//	public Select generateSQLStringDestOnly(Select dstStatement, boolean isSubTreeRoot, boolean stopAtJoin, Set<String> allowedScans) throws Exception {
//		
//		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans);
//
//		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
//		if (children.get(0) instanceof Join) {
//			ps.getSelectItems().clear();
//			for (String alias: outSchema.keySet()) {
//				Expression e = CCJSqlParserUtil.parseExpression(outSchema.get(alias).getExpressionString());
//				SelectItem s = new SelectExpressionItem(e);
//				if (!(e instanceof Column)) {
//					((SelectExpressionItem)s).setAlias(new Alias(alias));
//				}
//				ps.addSelectItems(s);
//			}
//		}
//		
//		ps.setDistinct(new net.sf.jsqlparser.statement.select.Distinct());
//		
//		return dstStatement;
//	}

	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
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
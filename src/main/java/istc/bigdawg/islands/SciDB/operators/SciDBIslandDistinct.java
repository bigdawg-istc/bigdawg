package istc.bigdawg.islands.SciDB.operators;

import java.util.LinkedHashMap;
import java.util.Map;

import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.islands.operators.Operator;

public class SciDBIslandDistinct extends SciDBIslandOperator implements Distinct {

	
	/**
	 * This class and hence this constructor is unused
	 * @param parameters
	 * @param output
	 * @param child
	 */
	
//	SciDBIslandDistinct(Map<String, String> parameters, List<String> output, SciDBIslandOperator child, SQLTableExpression supplement) throws Exception  {
//		super(parameters, output, child, supplement);
//		
//		
//		isBlocking = true;
//		blockerCount++;
//		this.blockerID = blockerCount;
//
//		if (children.get(0) instanceof SciDBIslandJoin) {
//			outSchema = new LinkedHashMap<>();
//			for(int i = 0; i < output.size(); ++i) {
//				String expr = output.get(i);
//				SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
//				SQLAttribute attr = out.getAttribute();
//				String attrName = attr.getName();
//				outSchema.put(attrName, attr);
//			}
//		} else {
//			outSchema = new LinkedHashMap<>(child.outSchema);
//		}
//		
//	}
	public SciDBIslandDistinct(Map<String, String> parameters, SciDBArray output, Operator child) {
		super(parameters, output, child);
		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		outSchema = new LinkedHashMap<String, DataObjectAttribute>(((SciDBIslandOperator)child).outSchema);
	}
	
	public SciDBIslandDistinct(SciDBIslandOperator o, boolean addChild) throws Exception {
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
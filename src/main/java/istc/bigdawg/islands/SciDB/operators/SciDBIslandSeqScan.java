package istc.bigdawg.islands.SciDB.operators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.SeqScan;

public class SciDBIslandSeqScan extends SciDBIslandScan implements SeqScan {

	// for AFL
	public SciDBIslandSeqScan (Map<String, String> parameters, SciDBArray output, Operator child) throws Exception  {
		super(parameters, output, child);
		
		setOperatorName(parameters.get("OperatorName"));
		
		Map<String, String> applyAttributes = new HashMap<>();
		if (parameters.get("Apply-Attributes") != null) {
			List<String> applyAttributesList = Arrays.asList(parameters.get("Apply-Attributes").split("@@@@"));
			for (String s : applyAttributesList) {
				String[] sSplit = s.split(" @AS@ ");
				applyAttributes.put(sSplit[1], sSplit[0]);
			}
		}
		
		// attributes
		for (String expr : output.getAttributes().keySet()) {
			
			DataObjectAttribute attr = new DataObjectAttribute();
			
			attr.setName(expr);
			attr.setTypeString(output.getAttributes().get(expr));
			attr.setHidden(false);
			
			String alias = attr.getName();
			if (!applyAttributes.isEmpty() && applyAttributes.get(expr) != null) attr.setExpression(applyAttributes.get(expr));
			else attr.setExpression(expr);
			
			outSchema.put(alias, attr);
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			DataObjectAttribute dim = new DataObjectAttribute();
			
			dim.setName(expr);
			dim.setTypeString(output.getAttributes().get(expr));
			dim.setHidden(true);
			
			String attrName = dim.getFullyQualifiedName();		
			
			outSchema.put(attrName, dim);
		}
		
	}
		
	public SciDBIslandSeqScan(SciDBIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		this.setOperatorName(((SciDBIslandSeqScan)o).getOperatorName());
	}

	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	
	public String toString() {
		return "SeqScan " + getSourceTableName() + " subject to (" + getFilterExpression()+")";
	}
	
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		
		if (isPruned() && (!isRoot)) {
			return "{PRUNED}";
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		if (children.isEmpty() && getOperatorName().equals("scan")){
			// it is a scan
			sb.append(this.getSourceTableName());
		} else if (children.isEmpty()) {
			sb.append(getOperatorName()).append('{').append(this.getSourceTableName()).append('}');
		} else {
			// filter, project
			sb.append(getOperatorName()).append(children.get(0).getTreeRepresentation(false));
		}
		sb.append('}');
		
		return sb.toString();
	}

	@Override
	public String getFullyQualifiedName() {
		return this.getSourceTableName();
	}
};
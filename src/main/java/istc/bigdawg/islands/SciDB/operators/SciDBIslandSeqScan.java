package istc.bigdawg.islands.SciDB.operators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.SciDB.SciDBAttributeOrDimension;
import istc.bigdawg.islands.SciDB.SciDBParsedArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;

public class SciDBIslandSeqScan extends SciDBIslandScan implements SeqScan {

	// for AFL
	public SciDBIslandSeqScan (Map<String, String> parameters, SciDBParsedArray output, Operator child) throws JSQLParserException {
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
			
			SciDBAttributeOrDimension attr = new SciDBAttributeOrDimension();
			
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
			
			SciDBAttributeOrDimension dim = new SciDBAttributeOrDimension();
			
			dim.setName(expr);
			dim.setTypeString(output.getDimensions().get(expr));
			dim.setHidden(true);
			
			String attrName = dim.getFullyQualifiedName();		
			
			outSchema.put(attrName, dim);
		}
		
	}
		
	public SciDBIslandSeqScan(SciDBIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		this.setOperatorName(((SciDBIslandSeqScan)o).getOperatorName());
	}

	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	
	public String toString() {
		return "SeqScan " + getSourceTableName() + " subject to (" + getFilterExpression()+")";
	}
	
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException{
		
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
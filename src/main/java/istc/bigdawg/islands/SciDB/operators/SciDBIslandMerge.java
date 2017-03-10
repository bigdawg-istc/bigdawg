package istc.bigdawg.islands.SciDB.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.SciDB.SciDBAttributeOrDimension;
import istc.bigdawg.islands.SciDB.SciDBParsedArray;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.shims.OperatorQueryGenerator;

public class SciDBIslandMerge extends SciDBIslandOperator implements Merge {

	private boolean isUnionAll = true; 
	
	// for AFL
	public SciDBIslandMerge(Map<String, String> parameters, SciDBParsedArray output, List<Operator> childs) throws Exception  {
		super(parameters, output, childs);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		outSchema = new LinkedHashMap<String, SciDBAttributeOrDimension>(((SciDBIslandOperator)childs.get(0)).outSchema);
	}
	
	public SciDBIslandMerge(SciDBIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		SciDBIslandMerge s = (SciDBIslandMerge) o;
		
		this.isUnionAll = s.isUnionAll;
		this.blockerID = s.blockerID;
	}
	
	
	public String toString() {
		if (isUnionAll) return "Union operator with UNION ALL option";
		else return "Union operator";
	}
	
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException{
		
		StringBuilder sb = new StringBuilder();
		sb.append('{').append("union");
		for (Operator o : children) {
			sb.append(o.getTreeRepresentation(false));
		}
		sb.append('}');
		
		
		return sb.toString();
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException{

		Map<String, Set<String>> out = children.get(0).getObjectToExpressionMappingForSignature();
		
		for (int i = 1 ; i < children.size() ; i++) {
			Map<String, Set<String>> temp = children.get(i).getObjectToExpressionMappingForSignature();
			for (String s : temp.keySet()) {
				if (out.containsKey(s)) 
					out.get(s).addAll(temp.get(s));
				else 
					out.put(s, temp.get(s));
			}
		}
		return out;
	}

	public boolean setUnionAll(boolean isUnionAll) {
		return this.isUnionAll = isUnionAll;
	}

	public boolean isUnionAll() {
		return this.isUnionAll;
	}
	
};
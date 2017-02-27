package istc.bigdawg.islands.SciDB.operators;

import java.util.LinkedHashMap;
import java.util.Map;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.shims.OperatorQueryGenerator;

public class SciDBIslandDistinct extends SciDBIslandOperator implements Distinct {

	
	/**
	 * This class and hence this constructor is unused
	 * @param parameters
	 * @param output
	 * @param child
	 */
	
	public SciDBIslandDistinct(Map<String, String> parameters, SciDBArray output, Operator child) {
		super(parameters, output, child);
		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		outSchema = new LinkedHashMap<String, DataObjectAttribute>(((SciDBIslandOperator)child).outSchema);
	}
	
	public SciDBIslandDistinct(SciDBIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
	}


	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	public String toString() {
		return "Distinct over " + outSchema;
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException{
		return "{distinct" + this.getChildren().get(0).getTreeRepresentation(isRoot)+"}";
	}
};
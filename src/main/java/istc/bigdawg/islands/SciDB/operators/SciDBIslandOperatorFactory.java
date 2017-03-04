package istc.bigdawg.islands.SciDB.operators;

import java.util.List;
import java.util.Map;

import istc.bigdawg.islands.SciDB.AFLQueryPlan;
import istc.bigdawg.islands.SciDB.SciDBParsedArray;
import istc.bigdawg.islands.operators.Operator;
import net.sf.jsqlparser.JSQLParserException;

public class SciDBIslandOperatorFactory {

	public static Operator get(String opType, Map<String, String> parameters, SciDBParsedArray output,  List<String> sortKeys, List<Operator> children, AFLQueryPlan plan) 
			throws JSQLParserException {
		
		switch (opType) {
			case "Aggregate":
				return new SciDBIslandAggregate(parameters, output, children.get(0));
			case "Cross Join":
				return new SciDBIslandJoin(parameters, output, children.get(0), children.get(1));
			case "Seq Scan":
				if (children.isEmpty())
					return new SciDBIslandSeqScan(parameters, output, null);
				else 
					return new SciDBIslandSeqScan(parameters, output, children.get(0));
			case "Sort":
				return new SciDBIslandSort(parameters, output, sortKeys, children.get(0));					
			case "WindowAgg":
				return new SciDBIslandWindowAggregate(parameters, output, children.get(0));
				
			default: // skip it, only designed for 1:1 io like hash and materialize
				System.out.println("Factory default trigger: "+opType);
				return children.get(0);
		}
		
	}
	
}

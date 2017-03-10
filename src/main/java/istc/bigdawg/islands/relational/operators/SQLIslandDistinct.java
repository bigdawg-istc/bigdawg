package istc.bigdawg.islands.relational.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.islands.relational.SQLOutItemResolver;
import istc.bigdawg.islands.relational.SQLTableExpression;
import istc.bigdawg.islands.relational.utils.SQLAttribute;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;

public class SQLIslandDistinct extends SQLIslandOperator implements Distinct {

	
	
	SQLIslandDistinct(Map<String, String> parameters, List<String> output, SQLIslandOperator child, SQLTableExpression supplement) throws QueryParsingException  {
		super(parameters, output, child, supplement);
		
		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		if (children.get(0) instanceof SQLIslandJoin) {
			outSchema = new LinkedHashMap<>();
			for(int i = 0; i < output.size(); ++i) {
				String expr = output.get(i);
				SQLOutItemResolver out; 
				try {
					out = new SQLOutItemResolver(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
				} catch (JSQLParserException e) {
					throw new QueryParsingException(e.getMessage(), e);
				}
				SQLAttribute attr = out.getAttribute();
				String attrName = attr.getName();
				outSchema.put(attrName, attr);
			}
		} else {
			outSchema = new LinkedHashMap<>(child.outSchema);
		}
		
	}

	
	public SQLIslandDistinct(SQLIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		
		this.blockerID = o.blockerID;
	}


	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	public String toString() {
		return "Distinct over " + outSchema;
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException {
		return "{distinct" + this.getChildren().get(0).getTreeRepresentation(isRoot)+"}";
	}
};
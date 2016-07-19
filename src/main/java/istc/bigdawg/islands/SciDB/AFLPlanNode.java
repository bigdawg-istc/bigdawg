package istc.bigdawg.islands.SciDB;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AFLPlanNode {
	
	/**
	 * >>>[lInstance] children 1
	 * >>>>[lOperator] filter ddl 0
	 *      [paramLogicalExpression] type bool const 0
	 *       [function] <= args:
	 *        [attributeReference] array  attr i
	 *        [constant] type int64 value 5
	 *      [opParamPlaceholder] PLACEHOLDER_INPUT requiredType void ischeme 1
	 *      [opParamPlaceholder] PLACEHOLDER_EXPRESSION requiredType bool ischeme 0
	 * >>>>schema: poe_med@1<poe_id:int64, ... dose_range_override:string> [ipoe_med=0:*,1000000,0]
	 */
	
	public String name = null;
	public List<AFLPlanAttribute> attributes;
	public SciDBArray schema = null;
	public Set<String> schemaAlias = null;
	public AFLPlanNode parent = null;
	public List<AFLPlanNode> children = null;
	public int childrenCount;
	public int indent;
	public int childrenReceived;
	
	public AFLPlanNode() {
		attributes = new ArrayList<>();
		children = new ArrayList<>();
		schemaAlias = new HashSet<>();
		childrenReceived = 0;
	}
	
	public void extractAliases() throws Exception {
		
		if (schema == null)
			throw new Exception("NULL schema from AFLPlanNode.");
		
		for (AFLPlanNode c : children) {
			c.extractAliases();
			this.schemaAlias.addAll(c.schemaAlias);
		}
		
		schema.getAllSchemaAliases().addAll(this.schemaAlias);
		
	}
	
	public void fixDimensionStrings() throws Exception {
		
		if (schema == null)
			throw new Exception("NULL schema from AFLPlanNode.");
		
		schema.fixDimensionStrings();
		
		for (AFLPlanNode c : children) {
			c.fixDimensionStrings();
		}
		
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		int l;
		
		sb.append("Level ").append(indent-2);
		sb.append(' ').append(name);
		sb.append("; Children: ");
		sb.append(childrenCount);
		sb.append('\n');
		l = attributes.size();
		for (int i = 0; i < l; ++i) sb.append(attributes.get(i));
		sb.append("Aliases: ").append(schemaAlias).append('\n');
		sb.append("Schema: ").append(schema.getSchemaString());
		sb.append('\n').append('\n');
		l = children.size();
		for (int i = 0; i < l; ++i) sb.append(children.get(i));
		
		
		return sb.toString();
	}
}

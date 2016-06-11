package istc.bigdawg.islands.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLOutItem;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;

public class Merge extends Operator {

	public enum MergeType {Intersect, Union};
	
	private boolean isUnionAll = true; 
	
	
	
	public Merge(Map<String, String> parameters, List<String> output, List<Operator> childs, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, childs, supplement);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		// Union ALL is set by the parent Aggregate or Unique
		
		if (children.get(0) instanceof Join) {
			outSchema = new LinkedHashMap<>();
			for(int i = 0; i < output.size(); ++i) {
				String expr = output.get(i);
				SQLOutItem out = new SQLOutItem(expr, childs.get(0).outSchema, supplement); // TODO CHECK THIS TODO
				SQLAttribute attr = out.getAttribute();
				String attrName = attr.getName();
				outSchema.put(attrName, attr);
			}
		} else {
			outSchema = new LinkedHashMap<>(childs.get(0).outSchema);
		}
		
	}
	
	// for AFL
	public Merge(Map<String, String> parameters, SciDBArray output, List<Operator> childs) throws Exception  {
		super(parameters, output, childs);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		outSchema = new LinkedHashMap<String, DataObjectAttribute>(childs.get(0).outSchema);
	}
	
	public Merge(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Merge s = (Merge) o;
		
		this.isUnionAll = s.isUnionAll;
		this.blockerID = s.blockerID;
	}
	
	
	public String toString() {
		if (isUnionAll) return "Union operator with UNION ALL option";
		else return "Union operator";
	}
	
	
	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		
		StringBuilder sb = new StringBuilder();
		sb.append('{').append("union");
		for (Operator o : children) {
			sb.append(o.getTreeRepresentation(false));
		}
		sb.append('}');
		
		
		return sb.toString();
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{

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
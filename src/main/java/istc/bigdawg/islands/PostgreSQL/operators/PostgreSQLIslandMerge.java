package istc.bigdawg.islands.PostgreSQL.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLOutItem;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.PostgreSQL.utils.SQLAttribute;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;

public class PostgreSQLIslandMerge extends PostgreSQLIslandOperator implements Merge {

	public enum MergeType {Intersect, Union};
	
	private boolean isUnionAll = true; 
	
	
	
	public PostgreSQLIslandMerge(Map<String, String> parameters, List<String> output, List<PostgreSQLIslandOperator> childs, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, childs, supplement);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		// Union ALL is set by the parent Aggregate or Unique
		
		if (children.get(0) instanceof PostgreSQLIslandJoin) {
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
	
//	// for AFL
//	public PostgreSQLIslandMerge(Map<String, String> parameters, SciDBArray output, List<PostgreSQLIslandOperator> childs) throws Exception  {
//		super(parameters, output, childs);
//
//		isBlocking = true;
//		blockerCount++;
//		this.blockerID = blockerCount;
//
//		outSchema = new LinkedHashMap<String, DataObjectAttribute>(childs.get(0).outSchema);
//	}
	
	public PostgreSQLIslandMerge(PostgreSQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		PostgreSQLIslandMerge s = (PostgreSQLIslandMerge) o;
		
		this.isUnionAll = s.isUnionAll;
		this.blockerID = s.blockerID;
	}
	
	public PostgreSQLIslandMerge() {
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
	};
	
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
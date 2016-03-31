package istc.bigdawg.plan.operators;

import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.SQLAttribute;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;


// TODO: expressions on aggregates - e.g., COUNT(*) / COUNT(v > 5)

public class Limit extends Operator {

	private boolean isLimitAll = false;
	private boolean isLimitNull = false;
	private boolean hasOffSet = false;
	private long limitCount = 0;
	private long limitOffset = 0;
	
	Limit(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		
		if (parameters.get("LimitAll") != null) isLimitAll = true;
		if (parameters.get("LimitNull") != null) isLimitNull = true;
		if (parameters.get("LimitCount") != null) limitCount = Long.parseLong(parameters.get("LimitCount"));
		if (parameters.get("LimitOffset") != null) {
			hasOffSet = true;
			limitOffset = Long.parseLong(parameters.get("LimitOffset"));
		}
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);

			
			SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
			SQLAttribute attr = out.getAttribute();
			String attrName = attr.getName();
			
			outSchema.put(attrName, attr);
			
		}

	}
	
	public Limit(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Limit lim = (Limit) o;
		
		
		this.isLimitAll = lim.isLimitAll;
		this.isLimitNull = lim.isLimitNull;
		this.hasOffSet = lim.hasOffSet;
		this.limitCount = lim.limitCount;
		this.limitOffset = lim.limitOffset;
		
	}
	
	
	// for AFL the constructor is undefined; because AFL doesn't support it
	
	
	public Limit() {
		isBlocking = true;
	}

	
	@Override
	public Select generateSQLStringDestOnly(Select dstStatement, Boolean stopAtJoin, Set<String> allowedScans) throws Exception {

		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, stopAtJoin, allowedScans);
				
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();

		net.sf.jsqlparser.statement.select.Limit sqllim = new net.sf.jsqlparser.statement.select.Limit();
		
		if (isLimitAll) sqllim.setLimitAll(isLimitAll);
		else if (isLimitNull) sqllim.setLimitNull(isLimitNull);
		else sqllim.setRowCount(limitCount);
		
		if (hasOffSet) sqllim.setOffset(limitOffset);
		
		ps.setLimit(sqllim);
		
		return dstStatement;
		
	}
	
	
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		if (isLimitAll)
			sb.append("Limit operator omitted by ALL paramter");
		else if (isLimitNull)
			sb.append("Limit operator omitted by NULL paramter");
		else
			sb.append("Limit operator preserves the first ").append(limitCount).append(" entries");
		
		if (hasOffSet)
			sb.append(" - after skipping ").append(limitOffset).append(" rows");
		
		return sb.toString();
	}
	
	@Override
	public String generateAFLString(int recursionLevel) throws Exception {
		
		// operator not applicable in AFL environement
		
		return children.get(0).generateAFLString(recursionLevel);
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else return "{limit"+children.get(0).getTreeRepresentation(false)+"}";
	}

};
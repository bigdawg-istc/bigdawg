package istc.bigdawg.plan.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.generators.OperatorVisitor;


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
		
		
		if (parameters.get("LimitAll") != null) setLimitAll(true);
		if (parameters.get("LimitNull") != null) setLimitNull(true);
		if (parameters.get("LimitCount") != null) setLimitCount(Long.parseLong(parameters.get("LimitCount")));
		if (parameters.get("LimitOffset") != null) {
			setHasOffSet(true);
			setLimitOffset(Long.parseLong(parameters.get("LimitOffset")));
		}
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		outSchema = new LinkedHashMap<>(child.outSchema);

	}
	
	public Limit(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Limit lim = (Limit) o;
		
		this.setLimitAll(lim.isLimitAll());
		this.setLimitNull(lim.isLimitNull());
		this.setHasOffSet(lim.isHasOffSet());
		this.setLimitCount(lim.getLimitCount());
		this.setLimitOffset(lim.getLimitOffset());
		
	}
	
	
	// for AFL the constructor is undefined; because AFL doesn't support it
	
	
	public Limit() {
		isBlocking = true;
	}

	
//	@Override
//	public Select generateSQLStringDestOnly(Select dstStatement, boolean isSubTreeRoot, boolean stopAtJoin, Set<String> allowedScans) throws Exception {
//
//		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans);
//				
//		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
//
//		net.sf.jsqlparser.statement.select.Limit sqllim = new net.sf.jsqlparser.statement.select.Limit();
//		
//		if (isLimitAll()) sqllim.setLimitAll(isLimitAll());
//		else if (isLimitNull()) sqllim.setLimitNull(isLimitNull());
//		else sqllim.setRowCount(getLimitCount());
//		
//		if (isHasOffSet()) sqllim.setOffset(getLimitOffset());
//		
//		ps.setLimit(sqllim);
//		
//		return dstStatement;
//		
//	}
	
	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		if (isLimitAll())
			sb.append("Limit operator omitted by ALL paramter");
		else if (isLimitNull())
			sb.append("Limit operator omitted by NULL paramter");
		else
			sb.append("Limit operator preserves the first ").append(getLimitCount()).append(" entries");
		
		if (isHasOffSet())
			sb.append(" - after skipping ").append(getLimitOffset()).append(" rows");
		
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

	public boolean isLimitAll() {
		return isLimitAll;
	}

	public void setLimitAll(boolean isLimitAll) {
		this.isLimitAll = isLimitAll;
	}

	public boolean isLimitNull() {
		return isLimitNull;
	}

	public void setLimitNull(boolean isLimitNull) {
		this.isLimitNull = isLimitNull;
	}

	public long getLimitCount() {
		return limitCount;
	}

	public void setLimitCount(long limitCount) {
		this.limitCount = limitCount;
	}

	public long getLimitOffset() {
		return limitOffset;
	}

	public void setLimitOffset(long limitOffset) {
		this.limitOffset = limitOffset;
	}

	public boolean isHasOffSet() {
		return hasOffSet;
	}

	public void setHasOffSet(boolean hasOffSet) {
		this.hasOffSet = hasOffSet;
	}

};
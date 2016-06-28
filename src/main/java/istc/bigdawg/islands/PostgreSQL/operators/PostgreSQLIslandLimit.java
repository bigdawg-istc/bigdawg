package istc.bigdawg.islands.PostgreSQL.operators;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLOutItemResolver;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.PostgreSQL.utils.SQLAttribute;
import istc.bigdawg.islands.operators.Limit;


public class PostgreSQLIslandLimit extends PostgreSQLIslandOperator implements Limit {

	private boolean isLimitAll = false;
	private boolean isLimitNull = false;
	private boolean hasOffSet = false;
	private long limitCount = 0;
	private long limitOffset = 0;
	
	PostgreSQLIslandLimit(Map<String, String> parameters, List<String> output, PostgreSQLIslandOperator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		net.sf.jsqlparser.statement.select.Limit l = supplement.getLimit();
		
		setLimitAll(l.isLimitAll());
		setLimitNull(l.isLimitNull());
		setLimitCount(l.getRowCount());
		if (l.getOffset() != 0) {
			setHasOffSet(true);
			setLimitOffset(l.getOffset());
		}
		
		if (children.get(0) instanceof PostgreSQLIslandJoin) {
			outSchema = new LinkedHashMap<>();
			for(int i = 0; i < output.size(); ++i) {
				String expr = output.get(i);
				SQLOutItemResolver out = new SQLOutItemResolver(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
				SQLAttribute attr = out.getAttribute();
				String attrName = attr.getName();
				outSchema.put(attrName, attr);
			}
		} else {
			outSchema = new LinkedHashMap<>(child.outSchema);
		}

//		System.out.printf("\n\n\n--> Limit constructor: output %s;\n outSchema %s;\n\n", output, outSchema);
	}
	
	public PostgreSQLIslandLimit(PostgreSQLIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		PostgreSQLIslandLimit lim = (PostgreSQLIslandLimit) o;
		this.blockerID = o.blockerID;
		this.setLimitAll(lim.isLimitAll());
		this.setLimitNull(lim.isLimitNull());
		this.setHasOffSet(lim.isHasOffSet());
		this.setLimitCount(lim.getLimitCount());
		this.setLimitOffset(lim.getLimitOffset());
		
	}
	
	
	// for AFL the constructor is undefined; because AFL doesn't support it
	
	
	public PostgreSQLIslandLimit() {
		super();
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
	}

	
	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}
	
	@Override
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		if (isLimitAll())
			sb.append("(Limit ALL");
		else if (isLimitNull())
			sb.append("(Limit NULL");
		else
			sb.append("(Limit ").append(getLimitCount());
		
		if (isHasOffSet())
			sb.append(", offset ").append(getLimitOffset());
		
		return sb.append(")").toString();
	}
	
//	@Override
//	public String generateAFLString(int recursionLevel) throws Exception {
//		
//		// operator not applicable in AFL environement
//		
//		return children.get(0).generateAFLString(recursionLevel);
//	}
	
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
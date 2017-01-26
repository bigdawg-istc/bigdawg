package istc.bigdawg.islands.text.operators;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Range;

import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.SeqScan;

public class TextScan extends TextOperator implements SeqScan {
	
	String tableName;
	Range range;
	
	public TextScan(String tableName, Range range) {
		super();
		this.tableName = tableName;
		this.range = range;
	}
	
	public Range getRange() {
		return range;
	}
	
	@Override
	public Operator duplicate(boolean addChild) throws Exception {
		return new TextScan(tableName, range);
	}

	@Override
	public Map<String, String> getDataObjectAliasesOrNames() throws Exception {
		Map<String, String> result = new HashMap<>();
		result.put(tableName, tableName);
		return result;
	}

	@Override
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws Exception {
	}

	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception {
		if (range.getStartKey() == null && range.getEndKey() == null)
			return String.format("(TextScan, %s, FULL_RANGE)", tableName);
		else 
			return String.format("(TextScan, %s%s%s)", 
					tableName, range.getStartKey() == null ? "" : 
						String.format(", (StartKey, %s, %s, %s)", range.getStartKey().getRow(), range.getStartKey().getColumnFamily(), range.getStartKey().getColumnQualifier()),
						range.getEndKey() == null ? "" : 
							String.format(", (EndKey, %s, %s, %s)", range.getEndKey().getRow(), range.getEndKey().getColumnFamily(), range.getEndKey().getColumnQualifier()));
	}

	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception {
		return new HashMap<>();
	}

	@Override
	public void accept(OperatorVisitor operatorVisitor) throws Exception {
		operatorVisitor.visit(this);
	}

	@Override
	public String getSourceTableName() {
		return tableName;
	}

	@Override
	public void setSourceTableName(String srcTableName) {
		this.tableName = srcTableName;
	}

	@Override
	public String generateRelevantJoinPredicate() throws Exception {
		return null;
	}

	@Override
	public String getFullyQualifiedName() {
		return tableName;
	}
	
}

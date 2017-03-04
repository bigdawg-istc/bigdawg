package istc.bigdawg.islands.SciDB.operators;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.SciDB.SciDBParsedArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

public class SciDBIslandScan extends SciDBIslandOperator implements Scan {
	
	private Expression filterExpression = null;
	private String srcTable = null;
	private String operatorName = null;
	private String arrayAlias;  //may be query-specific, need to derive it here
	
	// for AFL
	public SciDBIslandScan(Map<String, String> parameters, SciDBParsedArray output, Operator child) throws JSQLParserException {
		super(parameters, output, child);

		isBlocking = false;

		setSourceTableName(parameters.get("Relation-Name"));
		setArrayAlias(parameters.get("Alias"));
		
		if(parameters.get("Filter") != null) {
			setFilterExpression(CCJSqlParserUtil.parseCondExpression(parameters.get("Filter")));
		}

		
	}
	
	public SciDBIslandScan(SciDBIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		SciDBIslandScan sc = (SciDBIslandScan) o;
		
		if (sc.getFilterExpression() != null)
			try {
			this.setFilterExpression(CCJSqlParserUtil.parseCondExpression(sc.getFilterExpression().toString()));
			} catch (JSQLParserException e) {
				throw new IslandException(e.getMessage(), e);
			}
		this.setSourceTableName(new String(sc.getSourceTableName()));
		this.setArrayAlias(new String(sc.getArrayAlias()));
	}
	

	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException {
		
		if (! children.isEmpty()) return super.getObjectToExpressionMappingForSignature();
		
		Operator parent = this;
		while (!parent.isBlocking() && parent.getParent() != null ) parent = parent.getParent();
		Map<String, String> aliasMapping = parent.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = new HashMap<>();
		
		// filter
		try {
			if (getFilterExpression() != null && !SQLExpressionUtils.containsArtificiallyConstructedTables(getFilterExpression())) {
				addToOut(CCJSqlParserUtil.parseCondExpression(getFilterExpression().toString()), out, aliasMapping);
			}
		} catch(JSQLParserException ex) {
			throw new IslandException(ex.getMessage(), ex);
		}
		
		return out;
	}
	
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}

	public Expression getFilterExpression() {
		return filterExpression;
	}


	public void setFilterExpression(Expression filterExpression) {
		this.filterExpression = filterExpression;
	}


	public String getArrayAlias() {
		return arrayAlias;
	}


	public void setArrayAlias(String tableAlias) {
		this.arrayAlias = tableAlias;
	}


	public String getOperatorName() {
		return operatorName;
	}


	public void setOperatorName(String operatorName) {
		this.operatorName = operatorName;
	}


	@Override
	public String getSourceTableName() {
		return srcTable;
	}


	@Override
	public void setSourceTableName(String srcTableName) {
		this.srcTable = srcTableName;
	}


	@Override
	public String generateRelevantJoinPredicate() throws IslandException {
		// because there will not be a joinPredicated embedded
		return null;
	}

	
	
}

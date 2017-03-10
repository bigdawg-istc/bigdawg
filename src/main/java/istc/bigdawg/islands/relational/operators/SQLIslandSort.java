package istc.bigdawg.islands.relational.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Sort;
import istc.bigdawg.islands.relational.SQLOutItemResolver;
import istc.bigdawg.islands.relational.SQLTableExpression;
import istc.bigdawg.islands.relational.utils.SQLAttribute;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SQLIslandSort extends SQLIslandOperator implements Sort {

	
	private static Logger logger = Logger.getLogger(SQLIslandOperator.class);
	private List<String> sortKeys;
	
//	public enum SortOrder {ASC, DESC}; // ascending or descending?
	
	private SortOrder sortOrder;
	
	private List<OrderByElement> orderByElements;
//	private List<String> sortOrderStrings;
	
	private boolean isWinAgg = false; // is it part of a windowed aggregate or an ORDER BY clause?
	
	public SQLIslandSort(Map<String, String> parameters, List<String> output,  List<String> keys, SQLIslandOperator child, SQLTableExpression supplement) 
			throws QueryParsingException, JSQLParserException {
		super(parameters, output, child, supplement);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		// two order bys might exist in a supplement:
		// 1) within an OVER () clause for windowed aggregate
		// 2) as an ORDER BY clause
		// instantiate iterator to get the right one
		// iterate from first OVER --> ORDER BY
		
		sortOrder = supplement.getSortOrder(keys, parameters.get("sectionName"));
		setSortKeys(keys);

		if (children.get(0) instanceof SQLIslandJoin) {
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
		
		// match with previous schema to get any aliases to propagate
		for(int i = 0; i < getSortKeys().size(); ++i) {
			String a = supplement.getAlias(getSortKeys().get(i));
			if(a != null) 
				getSortKeys().set(i, a);
		}
		
		setOrderByElements(new ArrayList<>());
		
		// pick out the outitems that are not columns
		Map<String, String> outExps = new HashMap<>();
		for (String s : outSchema.keySet()) {
			if (!s.equals(outSchema.get(s).getExpressionString()))
				outExps.put(s, outSchema.get(s).getExpressionString());
		}
		
		for (int i = 0 ; i < getSortKeys().size() ; i++) {
//		for (String s : getSortKeys()) {
			
			String s = getSortKeys().get(i);
			Expression e = CCJSqlParserUtil.parseExpression(SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(s));
			SQLExpressionUtils.removeExcessiveParentheses(e);
			while (e instanceof Parenthesis) e = ((Parenthesis) e).getExpression();
			
			String estr = e.toString();
			
			estr = rewriteComplextOutItem(estr);
			
			OrderByElement obe = new OrderByElement();
			obe.setExpression(CCJSqlParserUtil.parseExpression(estr));
			
			OrderByElement userInputOBE = supplement.getOrderByClause().get(i);
			if (userInputOBE.isAscDescPresent()) {
				obe.setAscDescPresent(true);
				if (userInputOBE.isAsc()) {
					obe.setAsc(true);
				} else 
					obe.setAsc(false);
			} else {
				obe.setAscDescPresent(false);
			}
//			if (s.toUpperCase().endsWith("DESC")) {
//				obe.setAscDescPresent(true);
//				obe.setAsc(false);
//			} else if (s.toUpperCase().endsWith("ASC")) {
//				obe.setAscDescPresent(true);
//				obe.setAsc(true);
//			}
			getOrderByElements().add(obe);
		}
		
		logger.info(String.format("\n\n<<>> getOrderByElement from construction: %s; keys: %s\n\n\n", getOrderByElements(), keys));
	}
	
	public SQLIslandSort(SQLIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		SQLIslandSort s = (SQLIslandSort) o;
		
		this.blockerID = s.blockerID;

		this.setSortKeys(new ArrayList<>());
		this.setWinAgg(s.isWinAgg());
		for (String str : s.getSortKeys()) {
			this.getSortKeys().add(new String(str));
		}
		this.sortOrder = s.sortOrder; 
		this.setOrderByElements(new ArrayList<>());
		for (OrderByElement ob : s.getOrderByElements()) {
			this.getOrderByElements().add(ob);
		}
	}
	
	
	public String toString() {
		return "(sort "+children.get(0)+" " + getSortKeys().toString() + " " + sortOrder +")";
	}
	
	/**
	 * PRESERVED
	 * @return
	 * @throws Exception
	 */
	public List<OrderByElement> updateOrderByElements() throws Exception {
		
		List<OrderByElement> ret = new ArrayList<>();
		
		List<Operator> treeWalker = new ArrayList<>();
		for (OrderByElement obe : getOrderByElements()) {
			
			treeWalker.add(this);
			boolean found = false;
			
			while (treeWalker.size() > 0 && (!found)) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						
						Column c = (Column)obe.getExpression();
						
						if (((SQLIslandOperator) o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							OrderByElement newobe = new OrderByElement();
							newobe.setExpression(new Column(new Table(o.getPruneToken()), c.getColumnName()));
							ret.add(newobe);
							found = true;
							newobe.setAscDescPresent(obe.isAscDescPresent());
							if (obe.isAscDescPresent()) 
								newobe.setAsc(obe.isAsc());
							break;
						}
					} else {
						nextGeneration.addAll(o.getChildren());
					}
				}
				
				treeWalker = nextGeneration;
			}
			
			
		}
		
		if (ret.isEmpty()) ret.addAll(getOrderByElements());
		
		return ret;
	}
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException {
		return "{sort"+children.get(0).getTreeRepresentation(false)+"}";
	}

	public boolean isWinAgg() {
		return isWinAgg;
	}

	public void setWinAgg(boolean isWinAgg) {
		this.isWinAgg = isWinAgg;
	}

	public List<String> getSortKeys() {
		return sortKeys;
	}

	public void setSortKeys(List<String> sortKeys) {
		this.sortKeys = sortKeys;
	}

	public List<OrderByElement> getOrderByElements() {
		return orderByElements;
	}

	public void setOrderByElements(List<OrderByElement> orderByElements) {
		this.orderByElements = new ArrayList<>(orderByElements);
	}
	
};
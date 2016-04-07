package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Sort extends Operator {

	
	private List<String> sortKeys;
	
	public enum SortOrder {ASC, DESC}; // ascending or descending?
	
	private SortOrder sortOrder;
	
	private List<OrderByElement> orderByElements;
//	private List<String> sortOrderStrings;
	
	protected boolean isWinAgg = false; // is it part of a windowed aggregate or an ORDER BY clause?
	
	public Sort(Map<String, String> parameters, List<String> output,  List<String> keys, Operator child, SQLTableExpression supplement) throws Exception  {
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
		sortKeys = keys;
		outSchema = new LinkedHashMap<>(child.outSchema);
		
//		for (String s : outSchema.keySet()) {
//			DataObjectAttribute da = outSchema.get(s);
//			da.setExpression(rewriteComplextOutItem(da.getExpressionString()));
//		}

		// match with previous schema to get any aliases to propagate
		for(int i = 0; i < sortKeys.size(); ++i) {
			String a = supplement.getAlias(sortKeys.get(i));
			if(a != null) 
				sortKeys.set(i, a);
		}
		
		orderByElements = new ArrayList<>();
		
		// pick out the outitems that are not columns
		Map<String, String> outExps = new HashMap<>();
		for (String s : outSchema.keySet()) {
			if (!s.equals(outSchema.get(s).getExpressionString()))
				outExps.put(s, outSchema.get(s).getExpressionString());
		}
		
		for (String s : sortKeys) {
			Expression e = CCJSqlParserUtil.parseExpression(SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(s));
			SQLExpressionUtils.removeExcessiveParentheses(e);
			while (e instanceof Parenthesis) e = ((Parenthesis) e).getExpression();
			
			String estr = e.toString();
			
			estr = rewriteComplextOutItem(estr);
			
//			if (e instanceof Column && outExps.containsKey(estr))
//				e = CCJSqlParserUtil.parseExpression(outExps.get(estr));
//			else if (!(e instanceof Column) && outExps.containsValue(estr))
//				for (String str : outExps.keySet()) 
//					if (outExps.get(str).contains(estr))
//						e = new Column(str);
			
			OrderByElement obe = new OrderByElement();
			obe.setExpression(CCJSqlParserUtil.parseExpression(estr));
			if (s.endsWith("DESC")) {
				obe.setAscDescPresent(true);
				obe.setAsc(false);
			} else if (s.endsWith("ASC")) {
				obe.setAscDescPresent(true);
				obe.setAsc(true);
			}
			orderByElements.add(obe);
		}
		
//		orderByElements = supplement.getOrderByClause();
//		System.out.println("---> parameters.get(\"Sort-Key\")"+ parameters.get("Sort-Key"));
		
		// append all table names
//		for (int i = 0; i < orderByElements.size(); ++i) {
//			Column c = (Column)orderByElements.get(i).getExpression();
//			String[] s = sortKeys.get(i).split("\\.");
//			if (c.getColumnName().equals(s[s.length-1])) {
//				
//				switch (s.length) {
//				case 1:
//					// no need to change anything
//					break;
//				case 2:
//					c.setTable(new Table(s[0]));
//					break;
//				case 3:
//					c.setTable(new Table(s[0], s[1]));
//					break;
//				case 4:
//					c.setTable(new Table(new Database(s[0]), s[1], s[2]));
//					break;
//				default:
//					throw new Exception("Too many components in order by's sortkey; key: "+sortKeys.get(i));
//				}
//			} else {
//				System.out.println("--> Sort failure: srcSchema: "+outSchema);
//				throw new Exception("Elements mismatch between sortKeys and orderByElements: "+sortKeys+" "+orderByElements);
//			}
//		}
	
	}
	
	// for AFL
	public Sort(Map<String, String> parameters, SciDBArray output,  List<String> keys, Operator child) throws Exception  {
		super(parameters, output, child);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		// two order bys might exist in a supplement:
		// 1) within an OVER () clause for windowed aggregate
		// 2) as an ORDER BY clause
		// instantiate iterator to get the right one
		// iterate from first OVER --> ORDER BY

		sortKeys = keys;
		
		outSchema = new LinkedHashMap<String, DataObjectAttribute>(child.outSchema);
		
	}
	
	public Sort(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Sort s = (Sort) o;
		
		this.blockerID = s.blockerID;

		this.sortKeys = new ArrayList<>();
		this.isWinAgg = s.isWinAgg;
		for (String str : s.sortKeys) {
			this.sortKeys.add(new String(str));
		}
		this.sortOrder = s.sortOrder; 
		this.orderByElements = new ArrayList<>();
		for (OrderByElement ob : s.orderByElements) {
			this.orderByElements.add(ob);
		}
	}
	
	@Override
	public Select generateSQLStringDestOnly(Select dstStatement, boolean isSubTreeRoot, boolean stopAtJoin, Set<String> allowedScans) throws Exception {
		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans);

		updateOrderByElements();
		
		if(!isWinAgg) {
			((PlainSelect) dstStatement.getSelectBody()).setOrderByElements(orderByElements);
		}

		return dstStatement;

	}
	
	
	public String toString() {
		return "Sort operator on columns " + sortKeys.toString() + " with ordering " + sortOrder;
	}
	

	public void updateOrderByElements() throws Exception {
		
		List<Operator> treeWalker;
		for (OrderByElement obe : orderByElements) {
			
			treeWalker = children;
			boolean found = false;
			
			while (treeWalker.size() > 0 && (!found)) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						
						Column c = (Column)obe.getExpression();
						
						if (o.getOutSchema().containsKey(c.getFullyQualifiedName())) {
							c.setTable(new Table(o.getPruneToken()));
							found = true;
							break;
						}
					} else {
						nextGeneration.addAll(o.children);
					}
				}
				
				treeWalker = nextGeneration;
			}
			
			
		}
	}
	
	
	@Override
	public String generateAFLString(int recursionLevel) throws Exception{
		StringBuilder sb = new StringBuilder();
		sb.append("sort(");
		sb.append(children.get(0).generateAFLString(recursionLevel+1));
		if (!sortKeys.isEmpty()) {

			updateOrderByElements();
			
			for (OrderByElement obe: orderByElements) {
				sb.append(", ").append(obe.toString());
			}
			
		}
		sb.append(')');
		return sb.toString();
		
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		return "{sort"+children.get(0).getTreeRepresentation(false)+"}";
	}
	
};
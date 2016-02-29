package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.schema.DataObjectAttribute;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Database;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class Sort extends Operator {

	
	private List<String> sortKeys;
	
	public enum SortOrder {ASC, DESC}; // ascending or descending?
	
	private SortOrder sortOrder;
	
	private List<OrderByElement> orderByElements;
	private List<String> sortOrderStrings;
	
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
		
//		secureCoordination = children.get(0).secureCoordination;
		outSchema = new LinkedHashMap<String, DataObjectAttribute>(child.outSchema);
		
		
		// match with previous schema to get any aliases to propagate
		for(int i = 0; i < sortKeys.size(); ++i) {
			String a = supplement.getAlias(sortKeys.get(i));
			if(a != null) {
				sortKeys.set(i, a);
			}
			
			// if we sort on a protected or private key, then go to SMC
			// only simple expressions supported, no additional arithmetic ops
		}
		
		orderByElements = supplement.getOrderByClause();
		
		
		// append all table names
		for (int i = 0; i < orderByElements.size(); ++i) {
			Column c = (Column)orderByElements.get(i).getExpression();
			
			
			String[] s = sortKeys.get(i).split("\\.");
			
			if (c.getColumnName().equals(s[s.length-1])) {
				
				switch (s.length) {
				case 1:
					// no need to change anything
					break;
				case 2:
					c.setTable(new Table(s[0]));
					break;
				case 3:
					c.setTable(new Table(s[0], s[1]));
					break;
				case 4:
					c.setTable(new Table(new Database(s[0]), s[1], s[2]));
					break;
				default:
					throw new Exception("Too many components in order by's sortkey; key: "+sortKeys.get(i));
				}
			} else 
				throw new Exception("Elements mismatch between sortKeys and orderByElements: "+sortKeys+" "+orderByElements);
		}
	
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
	public Select generateSQLStringDestOnly(Select dstStatement) throws Exception {
		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement);

		updateOrderByElements();
		
		if(!isWinAgg) {
			PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
			ps.setOrderByElements(orderByElements);
			
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
	public String getTreeRepresentation(boolean isRoot){
		return "{sort"+children.get(0).getTreeRepresentation(false)+"}";
	}
	
};
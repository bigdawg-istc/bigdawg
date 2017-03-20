package istc.bigdawg.islands.SciDB.operators;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.SciDB.SciDBAttributeOrDimension;
import istc.bigdawg.islands.SciDB.SciDBParsedArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Sort;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SciDBIslandSort extends SciDBIslandOperator implements Sort {

	
//	private Map<String, SortOrder> sortKeys;
	
	private List<OrderByElement> orderByElements;
	
	private boolean isWinAgg = false; // is it part of a windowed aggregate or an ORDER BY clause?
	
	// for AFL
	public SciDBIslandSort(Map<String, String> parameters, SciDBParsedArray output,  List<String> keys, Operator child) {
		super(parameters, output, child);

		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;

		// two order bys might exist in a supplement:
		// 1) within an OVER () clause for windowed aggregate
		// 2) as an ORDER BY clause
		// instantiate iterator to get the right one
		// iterate from first OVER --> ORDER BY
		
		createOrderByElements(keys);
		
		outSchema = new LinkedHashMap<String, SciDBAttributeOrDimension>(((SciDBIslandOperator)child).outSchema);
		
	}
	
	public SciDBIslandSort(SciDBIslandOperator o, boolean addChild) throws IslandException {
		super(o, addChild);
		SciDBIslandSort s = (SciDBIslandSort) o;
		
		this.blockerID = s.blockerID;

		this.setOrderByElements(new ArrayList<>(s.getOrderByElements()));
		this.setWinAgg(s.isWinAgg());
//		this.sortOrder = s.sortOrder; 
//		this.setOrderByElements(new ArrayList<>());
//		for (OrderByElement ob : s.getOrderByElements()) {
//			this.getOrderByElements().add(ob);
//		}
	}
	
	
	public String toString() {
		return "Sort operator on columns " + getOrderByElements().toString();
	}
	
	public void createOrderByElements(List<String> keys) {
		orderByElements = new ArrayList<>();
		for (String s : keys) {
			String[] splits = s.split("[.]");
			Column c; 
			if (splits.length == 2)
				c = new Column(new Table(splits[0]), splits[1]);
			else 
				c = new Column(splits[0]);
			OrderByElement ob = new OrderByElement();
			ob.setExpression(c);
			orderByElements.add(ob);
		}
	}
	
	/**
	 * PRESERVED
	 * @return
	 * @throws Exception
	 */
	public List<OrderByElement> updateOrderByElements() throws Exception {
		
		List<OrderByElement> ret = new ArrayList<>();
		
		List<Operator> treeWalker;
		for (OrderByElement obe : getOrderByElements()) {
			
			treeWalker = children;
			boolean found = false;
			
			while (treeWalker.size() > 0 && (!found)) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						
						Column c = (Column)obe.getExpression();
						
						if (((SciDBIslandOperator)o).getOutSchema().containsKey(c.getFullyQualifiedName())) {
							OrderByElement newobe = new OrderByElement();
							newobe.setExpression(new Column(new Table(o.getPruneToken()), c.getColumnName()));
							ret.add(newobe);
							found = true;
							break;
						}
					} else {
						nextGeneration.addAll(o.getChildren());
					}
				}
				
				treeWalker = nextGeneration;
			}
			
			
		}
		
		return ret;
	}
	
	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException{
		return "{sort"+children.get(0).getTreeRepresentation(false)+"}";
	}

	public boolean isWinAgg() {
		return isWinAgg;
	}

	public void setWinAgg(boolean isWinAgg) {
		this.isWinAgg = isWinAgg;
	}

//	public Map<String, SortOrder> getSortKeys() {
//		return sortKeys;
//	}
//
//	public void setSortKeys(Map<String, SortOrder> sortKeys) {
//		this.sortKeys = new HashMap<>(sortKeys);
//	}

	public List<OrderByElement> getOrderByElements() {
		return orderByElements;
	}

	public void setOrderByElements(List<OrderByElement> orderByElements) {
		this.orderByElements = orderByElements;
	}
	
};
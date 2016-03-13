package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jcp.xml.dsig.internal.dom.Utils;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.plan.extract.CommonOutItem;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.utils.sqlutil.SQLUtilities;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;


// TODO: expressions on aggregates - e.g., COUNT(*) / COUNT(v > 5)

public class Aggregate extends Operator {

	// can address complex expressions by adding a step after aggregate
	// create a list of aggregations to perform
	
	public enum AggregateType { MIN, MAX, COUNT, COUNT_DISTINCT, AVG, SUM};
	private List<SQLAttribute> groupBy;
	private List<String> aggregateExpressions; // e.g., COUNT(SOMETHING)
	private List<AggregateType>  aggregates; 
	private List<String> aggregateAliases; 
	private List<Function> parsedAggregates;
	private List<Expression> parsedGroupBys;
	private String aggregateFilter = null; // HAVING clause
	
	
	// TODO: write ObliVM aggregate as a for loop over values, 
	// maintain state once per aggregate added
	// apply any expressions down the line in the final selection
	
	Aggregate(Map<String, String> parameters, List<String> output, Operator child, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, child, supplement);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		aggregateAliases = new ArrayList<String>(); 
		groupBy = new ArrayList<SQLAttribute>();
	
		parsedAggregates = new ArrayList<Function>();
		parsedGroupBys = supplement.getGroupBy();
		aggregateFilter = parameters.get("Filter");
		if(aggregateFilter != null) {
			aggregateFilter = Utils.parseIdFromSameDocumentURI(aggregateFilter); // HAVING clause
		}
		
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);

			
			SQLOutItem out = new SQLOutItem(expr, child.outSchema, supplement); // TODO CHECK THIS TODO
			SQLAttribute attr = out.getAttribute();
			String attrName = attr.getName();
			
			
			outSchema.put(attrName, attr);
			
			
			// e.g., sum(y) / count(x)
			if(out.hasAggregate()) {
				List<Function> parsedAggregates = out.getAggregates();
				
				for(int j = 0; j < parsedAggregates.size(); ++j) {
					processFunction(parsedAggregates.get(j), attrName);
				}
				
				
			}
			else {
				groupBy.add(attr);
				/*if(attr.getSecurityPolicy() != Attribute.SecurityPolicy.Public) {
					throw new Exception("Aggregation must only group by public attributes.");
				}*/
			}
			
		}

	}
	
	public Aggregate(Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Aggregate a = (Aggregate) o;
		
		this.groupBy = new ArrayList<SQLAttribute>();
		this.aggregateExpressions = new ArrayList<String>(); // e.g., COUNT(SOMETHING)
		this.aggregates = new ArrayList<AggregateType>(); 
		this.aggregateAliases = new ArrayList<String> (); 
		this.parsedAggregates = new ArrayList<Function>();
		this.parsedGroupBys = new ArrayList<Expression>();
		if (a.aggregateFilter != null)
			this.aggregateFilter = new String (a.aggregateFilter); // HAVING clause
		
		for (SQLAttribute att : a.groupBy)
			this.groupBy.add(new SQLAttribute(att));
		for (String ae : a.aggregateExpressions)
			this.aggregateExpressions.add(new String(ae));
		for (AggregateType at : a.aggregates)
			this.aggregates.add(at);
		for (String aa : a.aggregateAliases)
			this.aggregateAliases.add(new String (aa));
		for (Function pa : a.parsedAggregates) {
			Function f = new Function();
			f.setAllColumns(pa.isAllColumns());
			f.setAttribute(new String(pa.getAttribute()));
			f.setDistinct(pa.isDistinct());
			f.setEscaped(pa.isEscaped());
			f.setKeep(pa.getKeep());
			f.setName(new String(pa.getName()));
			f.setParameters(pa.getParameters());
			this.parsedAggregates.add(f);
		}
		for (Expression e : a.parsedGroupBys)
			this.parsedGroupBys.add(e);
		
	}
	
	
	// for AFL
	Aggregate(Map<String, String> parameters, SciDBArray output, Operator child) throws Exception  {
		super(parameters, output, child);

		
		isBlocking = true;
		blockerCount++;
		this.blockerID = blockerCount;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		aggregateAliases = new ArrayList<String>(); 
		groupBy = new ArrayList<SQLAttribute>();
	
		parsedAggregates = new ArrayList<Function>();
//		parsedGroupBys = supplement.getGroupBy();
		aggregateFilter = parameters.get("Filter");
//		if(aggregateFilter != null) {
//			aggregateFilter = Utils.parseIdFromSameDocumentURI(aggregateFilter); // HAVING clause
//		}
		
		
		// iterate over outschema and 
		// classify each term as aggregate func or group by
		for (String expr : output.getAttributes().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getAttributes().get(expr), true, null); // TODO CHECK THIS TODO
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getName();
			
			outSchema.put(attrName, attr);
			
			
			// e.g., sum(y) / count(x)
//			if(out.hasAggregate()) {
//				List<Function> parsedAggregates = out.getAggregates();
//				for(int j = 0; j < parsedAggregates.size(); ++j) {
//					processFunction(parsedAggregates.get(j), attrName);
//				}
//				
//				
//			}
//			else {
////				groupBy.add(attr);
//			}
			
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, "Dimension", true, null);
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		

	}
	
	
	void processFunction(Function f, String alias) throws Exception  {
		switch(f.getName()) {
			case "min":
				aggregates.add(AggregateType.MIN);
				break;
			case "max":
				aggregates.add(AggregateType.MAX);
				break;
			case "avg":
				aggregates.add(AggregateType.AVG);
				break;
			case "sum":
				aggregates.add(AggregateType.SUM);
				break;
			case "count":
				if(f.isDistinct())  {
						aggregates.add(AggregateType.COUNT_DISTINCT); }
				else {
					aggregates.add(AggregateType.COUNT); }
				break;
			default:
				throw new Exception("Unknown aggregate type " + f.getName());
		}

		if(f.getParameters() != null) {
			String parameter = f.getParameters().toString();
			aggregateExpressions.add(parameter);
			parameter = SQLUtilities.removeOuterParens(parameter);
			// check for secure coordination
//			SQLAttribute attr = children.get(0).outSchema.get(parameter);
			
//			if(attr != null) {
//				updateSecurityPolicy(attr);
//			}
		}
		else {
			aggregateExpressions.add("");
		}
		
		aggregateAliases.add(alias);
			
	}
	
	public Aggregate() {
		isBlocking = true;
		
		aggregates = new ArrayList<AggregateType>();
		aggregateExpressions = new ArrayList<String>(); 
		
		
	}

	
	public void addAggregate(AggregateType a, String aFilter) {
		aggregates.add(a);
		aggregateExpressions.add(aFilter);
	}
	
	
	@Override
	public Select generateSQLStringDestOnly(Select dstStatement, boolean stopAtJoin) throws Exception {

		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, stopAtJoin);
				
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();

		if (ps.getSelectItems().get(0) instanceof AllColumns)
			ps.getSelectItems().remove(0);
		for (String alias: outSchema.keySet()) {
			
			
			Expression e = outSchema.get(alias).getSQLExpression();
			SelectItem s = new SelectExpressionItem(e);
			
			if (!(e instanceof Column))
				((SelectExpressionItem)s).setAlias(new Alias(alias));
			
			ps.addSelectItems(s);
		}
		
		updateGroupByElements();
		
		ps.setGroupByColumnReferences(parsedGroupBys);
		
		return dstStatement;
		
	}
	
	
	public void updateGroupByElements() throws Exception {
		
		List<Operator> treeWalker;
		if (parsedGroupBys == null)
			return;
		for (Expression gb : parsedGroupBys) {
			
			treeWalker = children;
			boolean found = false;
			
			while (treeWalker.size() > 0 && (!found)) {
				List<Operator> nextGeneration = new ArrayList<>();
				
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						
						Column c = (Column)gb;
						
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
	public String toString() {
		return "Aggregating on " + aggregateExpressions.toString() + " group by " + groupBy + " types " + aggregates.toString();
	}
	
	@Override
	public String generateAFLString(int recursionLevel) throws Exception {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("Aggregate(");
		sb.append(children.get(0).generateAFLString(recursionLevel+1));
		
		// TODO make sure the GroupBy are marked as hidden, otherwise do a redimension
		
//		System.out.println("outSchema:: "+outSchema);
//		System.out.println("AFL schema:: "+this.generateAFLCreateArrayStatementLocally("AAA"));
		
		for(int i = 0; i < aggregates.size(); ++i) {
			sb.append(", ").append(aggregates.get(i)).append(aggregateExpressions.get(i));
			if (aggregateAliases.get(i) != null)
				sb.append(" AS ").append(aggregateAliases.get(i));
			
		}
		updateGroupByElements();
		
		if(groupBy.size() > 0) {
			for(int i = 0; i < groupBy.size(); ++i) {
				sb.append(", ").append(groupBy.get(i).getName());
			}
		}

		sb.append(')');
		
		return sb.toString();
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot){
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else return "{aggregate"+children.get(0).getTreeRepresentation(false)+"}";
	}

};
package istc.bigdawg.islands.SciDB.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

public class SciDBIslandJoin extends SciDBIslandOperator implements Join {

//	public enum JoinType  {Left, Natural, Right};
	
	private JoinType joinType = null;
	private String joinPredicate = null;
//	private String joinFilter = null; 
	private List<String> aliases;
	
	protected Map<String, DataObjectAttribute> srcSchema;
	
	protected static final String BigDAWGSciDBJoinPrefix = "BIGDAWGSCIDBJOIN_";
	protected static int maxJoinSerial = 0;
	protected Integer joinID = null;
	
	
	// for AFL
	public SciDBIslandJoin(Map<String, String> parameters, SciDBArray output, Operator lhs, Operator rhs) throws Exception  {
		super(parameters, output, lhs, rhs);

		maxJoinSerial++;
		this.setJoinID(maxJoinSerial);
		
		isBlocking = false;
		
		joinPredicate = parameters.get("Join-Predicate"); System.out.printf("--> Join AFL Constructor, Join Predicate: %s\n", joinPredicate);
		setAliases(Arrays.asList(parameters.get("Children-Aliases").split(" ")));

		srcSchema = new LinkedHashMap<String, DataObjectAttribute>(((SciDBIslandOperator)lhs).outSchema);
		srcSchema.putAll(((SciDBIslandOperator)rhs).outSchema);
		
		// attributes
		for (String expr : output.getAttributes().keySet()) {
			
			DataObjectAttribute attr = new DataObjectAttribute();
			
			attr.setName(expr);
			attr.setTypeString(output.getAttributes().get(expr));
			attr.setHidden(false);
			
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			DataObjectAttribute dim = new DataObjectAttribute(); // CommonOutItemResolver out = new CommonOutItemResolver(expr, "Dimension", true, srcSchema);
			
			dim.setName(expr);
			dim.setTypeString(output.getAttributes().get(expr));
			dim.setHidden(true);
			
			String attrName = dim.getFullyQualifiedName();		
			
			outSchema.put(attrName, dim);
				
		}
	}
	
 	public SciDBIslandJoin (SciDBIslandOperator o, boolean addChild) throws Exception {
		super(o, addChild);
		SciDBIslandJoin j = (SciDBIslandJoin) o;
		
		this.setJoinID(j.getJoinID());
		
		this.joinType = j.joinType;
		this.isCopy = j.isCopy;
		
		if (j.joinPredicate == null) this.joinPredicate = j.joinPredicate;
		else this.joinPredicate = new String(j.joinPredicate);
		
//		if (j.joinFilter == null) this.joinFilter = j.joinFilter;
//		else this.joinFilter = new String(j.joinFilter);

		this.srcSchema = new HashMap<>();
		for (String s : j.srcSchema.keySet()) {
			if (j.srcSchema.get(s) != null) 
				this.srcSchema.put(new String(s), new DataObjectAttribute(j.srcSchema.get(s)));
		}
		
		this.setAliases(new ArrayList<>());
		if (j.getAliases() != null) {
			for (String a : j.getAliases()) {
				this.getAliases().add(new String(a));
			}
		}
	}
	
	public SciDBIslandJoin(SciDBIslandOperator child0, SciDBIslandOperator child1, JoinType jt, String joinPred, boolean isFilter) throws JSQLParserException {
		this.isCTERoot = false; // TODO VERIFY
		this.isBlocking = false; 
		this.isPruned = false;
		this.isCopy = true;
		this.setAliases(new ArrayList<>());
		
		maxJoinSerial++;
		this.setJoinID(maxJoinSerial);
		 
		if (jt != null) this.joinType = jt;
		
		this.joinPredicate = new String(joinPred);
		
		this.isQueryRoot = true;
		
		this.srcSchema = new LinkedHashMap<String, DataObjectAttribute>(child0.outSchema);
		srcSchema.putAll(child1.outSchema);
		
		this.outSchema = new LinkedHashMap<String, DataObjectAttribute>(child0.outSchema);
		outSchema.putAll(child1.outSchema);
		
		
		this.children = new ArrayList<>();
		this.children.add(child0);
		this.children.add(child1);
		
		if (child0.isCopy()) child0.parent = this;
		if (child1.isCopy()) child1.parent = this;
		
		child0.isQueryRoot = false;
		child1.isQueryRoot = false;
	}
	
	public SciDBIslandJoin() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public Join construct(Operator child0, Operator child1, JoinType jt, String joinPred, boolean isFilter) throws JSQLParserException {
		return new SciDBIslandJoin((SciDBIslandOperator) child0, (SciDBIslandOperator) child1, jt, joinPred, isFilter);
	};
	

    @Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}
    
    
    
    public String toString() {
    		return "Joining " + children.get(0).toString() + " x " + children.get(1).toString() 
    				+ " type " + joinType + " predicates " + joinPredicate;
    }
    
	
	public String getOriginalJoinPredicate() {
		return joinPredicate != null ? new String(joinPredicate) : null;
	}
	
	public String getOriginalJoinFilter() {
		return null;
	}
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else {
			StringBuilder sb = new StringBuilder();
			sb.append("{join").append(children.get(0).getTreeRepresentation(false)).append(children.get(1).getTreeRepresentation(false));

			sb.append('}');
			return sb.toString();
		}
	}
	
	
	public String getJoinToken() {
		
		if (getJoinID() == null) {
			maxJoinSerial ++;
			setJoinID(maxJoinSerial);
		}
		
		return BigDAWGSciDBJoinPrefix + getJoinID();
	}
	
	
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		
		Operator parent = this;
		while (!parent.isBlocking() && parent.getParent() != null ) parent = parent.getParent();
		Map<String, String> aliasMapping = this.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = children.get(0).getObjectToExpressionMappingForSignature();
		Map<String, Set<String>> temp = children.get(1).getObjectToExpressionMappingForSignature();
		
		for (String s : temp.keySet()) {
			if (out.containsKey(s)) 
				out.get(s).addAll(temp.get(s));
			else 
				out.put(s, temp.get(s));
		}
		// joinPredicate
		if (joinPredicate != null) { 
			Expression e = CCJSqlParserUtil.parseCondExpression(joinPredicate);
			if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e))
				addToOut(e, out, aliasMapping);
		}
		
		return out;
	}
	
	

	public Integer getJoinID() {
		return joinID;
	}


	public void setJoinID(Integer joinID) {
		this.joinID = joinID;
	}


	public List<String> getAliases() {
		return aliases;
	}


	public void setAliases(List<String> aliases) {
		this.aliases = aliases;
	}

	@Override
	public String generateJoinPredicate() throws Exception {
		// TODO ensure correctness
		return new String(joinPredicate);
	}

	@Override
	public String generateJoinFilter() throws Exception {
		return null;
	}


	
};

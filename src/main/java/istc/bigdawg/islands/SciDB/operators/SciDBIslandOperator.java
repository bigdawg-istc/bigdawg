package istc.bigdawg.islands.SciDB.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.SciDB.SciDBArray;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.utils.SQLAttribute;
import istc.bigdawg.islands.relational.utils.SQLExpressionUtils;
import istc.bigdawg.shims.OperatorQueryGenerator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;

public class SciDBIslandOperator implements Operator {

	static final String BigDAWGSciDBPrunePrefix = "BIGDAWGSCIDBPRUNED_"; 
	static final String BigDAWGSciDBSubtreePrefix = "BIGDAWGSCIDBSUBTREE_";
	
	protected boolean isCTERoot = false;
	// for use in getPlaintext

	
	// does this op need access to all inputs before it can emit output?
	// e.g., max, min, sort
	// these block force sync points in our setup
	protected boolean isBlocking = false; 
	protected static int blockerCount = 0;
	protected Integer blockerID = null;
	
	protected boolean isPruned = false;
	protected static int pruneCount = 0;
	protected Integer pruneID = null;
	
	protected boolean isSubTree = false;
	protected static int subTreeCount = 0;
	protected Integer subTreeID = null;
	
	
	protected Map<String, DataObjectAttribute> outSchema;
	
	// direct descendants
	protected List<Operator> children;
	protected Operator parent = null;
	
	protected boolean isQueryRoot = false;
	
	private Set<String> objectAliases = null;
	protected boolean isCopy = false;  // used in building permutations; only remainder join operators could attain true, so far
	

	// for AFL
	public SciDBIslandOperator(Map<String, String> parameters, SciDBArray output,  
			Operator child) {

		// order preserving
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		
		if(child != null) { // check for leaf nodes
			children.add(child);
			child.setParent(this);
			
		}
		
	}
	
	
	// for AFL
	public SciDBIslandOperator(Map<String, String> parameters, SciDBArray output, 
			Operator lhs, Operator rhs) {
		
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		
		children.add(lhs);
		children.add(rhs);

		lhs.setParent(this);
		rhs.setParent(this);

	}
	
	// for AFL UNION
	public SciDBIslandOperator(Map<String, String> parameters, SciDBArray output,  
			List<Operator> childs) {

		// order preserving
		outSchema = new LinkedHashMap<String, DataObjectAttribute>();
		children  = new ArrayList<Operator>();
		
		for (Operator o : childs) { // check for leaf nodes
			children.add(o);
			o.setParent(this);
			
		}
		
	}
	
	public SciDBIslandOperator() {
		
	}
	
	public SciDBIslandOperator(SciDBIslandOperator o, boolean addChild) throws IslandException {
		
		this.isCTERoot = o.isCTERoot;
		this.isBlocking = o.isBlocking; 
		this.isPruned = o.isPruned;
		this.pruneID = o.pruneID;

		this.isQueryRoot = o.isQueryRoot;
		
		this.outSchema = new LinkedHashMap<>();
		try {
			for (String s : o.outSchema.keySet()) {
				
				if (o.outSchema.get(s) instanceof SQLAttribute) {
					this.outSchema.put(new String(s), new SQLAttribute((SQLAttribute)o.outSchema.get(s)));
				} else {
					this.outSchema.put(new String(s), new DataObjectAttribute(o.outSchema.get(s)));
				}
			}
		} catch (JSQLParserException e) {
			throw new IslandException(e.getMessage(), e);
		}
		
		this.children = new ArrayList<>();
		
		
		if (addChild) {
			for (Operator s : o.children) {
				
				// pruned nodes are not regenerated
				if (s.isPruned()) {
					this.children.add(s);
					continue;
				}
				
				Operator op = s.duplicate(addChild);
				op.setParent(this);
				this.children.add(op);
				
			}
		}
	}

	
	@Override
	public boolean isCTERoot() {
		return isCTERoot;
	}

	@Override
	public void setCTERoot(boolean b) {
		isCTERoot = b;
	}
	
	@Override
	public void setParent(Operator p) {
		parent = p;
	}
	
	@Override
	public Operator getParent() {
		return parent;
	}

	@Override
	public List<Operator> getChildren() {
		return children;
	}
	
	@Override
	public void addChild(Operator aChild) {
		children.add(aChild);
	}

	@Override
	public void addChilds(Collection<Operator> childs) {
		children.addAll(childs);
	}
		
	
	public boolean isAnyProgenyPruned() {
		if (this.isPruned) return true;
		else if (children.isEmpty()) return false;
		else return ((SciDBIslandOperator) children.get(0)).isAnyProgenyPruned();
	}
	
	@Override
	public Map<String, DataObjectAttribute>  getOutSchema() {
		return outSchema;
	}
	
	// if it is blocking, this operator changes our SMC control flow
	// e.g., c-diff can't complete part of the self-join locally because it has a sort that needs to run first.
	// other joins can do partial matches locally
	// can keep passing around partial results until we hit a blocking operator
	// trace the path of the data through plan nodes
	public boolean isBlocking() {
		return isBlocking;
	}
	
	
	@Override
	public boolean isPruned() {
		return isPruned;
	}
	
	@Override
	public void prune(boolean p) {
		if (p && this.pruneID == null) {
			pruneCount += 1;
			this.pruneID = pruneCount;
		}
		isPruned = p;
	}
	
	@Override
	public String getPruneToken() throws IslandException {
		if (!isPruned) 
			throw new IslandException("\n\n\n----> unpruned token: "+this.outSchema+"\n\n");
		return BigDAWGSciDBPrunePrefix + this.pruneID;
	}
	
	@Override
	public boolean isSubTree() {
		return this.isSubTree;
	}
	
	@Override
	public void setSubTree(boolean t) {
		if (this instanceof SciDBIslandJoin) return;
		if (t && this.subTreeID == null) {
			subTreeCount += 1;
			this.subTreeID = subTreeCount;
		}
		isSubTree = t;
	}
	
	@Override
	public String getSubTreeToken() throws IslandException {
		if (!isSubTree && !(this instanceof SciDBIslandJoin)) return null;
		if (this instanceof SciDBIslandJoin) return ((SciDBIslandJoin)this).getJoinToken(); 
		else if (this instanceof SciDBIslandAggregate && ((SciDBIslandAggregate)this).getAggregateID() != null) return ((SciDBIslandAggregate)this).getAggregateToken();
		else return BigDAWGSciDBSubtreePrefix + this.subTreeID;
	}
	
	@Override
	public void setQueryRoot(boolean isRoot) {
		this.isQueryRoot = isRoot;
	}
	
	@Override
	public boolean isQueryRoot() {
		return this.isQueryRoot;
	}
	
	@Override
	public Map<String, String> getDataObjectAliasesOrNames() throws IslandException {
		
		Map<String, String> aliasOrString = new LinkedHashMap<>();
		
		boolean masked = false;
		String replaceToken = null;
		
		if (isSubTree) {
			replaceToken = getSubTreeToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (isPruned) {
			replaceToken = getPruneToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
			
		}
		
		if (this instanceof SciDBIslandJoin && ((SciDBIslandJoin)this).joinID != null) {
			replaceToken = ((SciDBIslandJoin)this).getJoinToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (this instanceof SciDBIslandAggregate && ((SciDBIslandAggregate)this).getAggregateID() != null) {
			replaceToken = ((SciDBIslandAggregate)this).getAggregateToken();
			aliasOrString.put(replaceToken, replaceToken);
			masked = true;
		}
		
		if (this.children.size() > 0 ) {
			
			if (this instanceof SciDBIslandScan) {
				if (((SciDBIslandScan)this).getArrayAlias() != null && !((SciDBIslandScan)this).getArrayAlias().isEmpty())
					aliasOrString.put(((SciDBIslandScan)this).getArrayAlias(), ((SciDBIslandScan)this).getSourceTableName());
				else 
					aliasOrString.put(((SciDBIslandScan)this).getSourceTableName(), ((SciDBIslandScan)this).getSourceTableName());
			}
			
			for (Operator o : children) {
				aliasOrString.putAll(o.getDataObjectAliasesOrNames());
				if (masked) {
					for (String s : o.getDataObjectAliasesOrNames().keySet()) {
						aliasOrString.replace(s, replaceToken);
					}
				}
			}
		} else {
			if (((SciDBIslandScan)this).getArrayAlias() != null && !((SciDBIslandScan)this).getArrayAlias().isEmpty())
				aliasOrString.put(((SciDBIslandScan)this).getArrayAlias(), ((SciDBIslandScan)this).getSourceTableName());
			else 
				aliasOrString.put(((SciDBIslandScan)this).getSourceTableName(), ((SciDBIslandScan)this).getSourceTableName());
		}
		
		return aliasOrString;
	}
	
	public List<Operator> getDataObjects() {
		List<Operator> extraction = new ArrayList<>();
		
		if (isPruned) {
			extraction.add(this);
			return extraction;
		}
		
		if (!(this instanceof SciDBIslandScan )) {
			for (Operator o : children) {
				extraction.addAll(((SciDBIslandOperator) o).getDataObjects());
			}
		} else {
			extraction.add(this);
		}
		
		
		return extraction;
	}
	
	@Override
	public boolean isCopy(){
		return this.isCopy;
	};
	
	@Override
	public Integer getBlockerID() throws IslandException {
		if (!isBlocking)
			throw new IslandException("SciDBIslandOperator Not blocking: "+this.toString());
		return blockerID;
	}
	
	@Override
	public Operator duplicate(boolean addChild) throws IslandException {
		try {
			if (this instanceof SciDBIslandJoin) {
				return new SciDBIslandJoin(this, addChild);
			} else if (this instanceof SciDBIslandSeqScan) {
				return new SciDBIslandSeqScan(this, addChild);
			} else if (this instanceof SciDBIslandSort) {
				return new SciDBIslandSort(this, addChild);
			} else if (this instanceof SciDBIslandAggregate) {
				return new SciDBIslandAggregate(this, addChild);
	//		} else if (this instanceof SciDBIslandLimit) {
	//			return new SciDBIslandLimit(this, addChild);
			} else if (this instanceof SciDBIslandDistinct) {
				return new SciDBIslandDistinct(this, addChild);
			} else if (this instanceof SciDBIslandMerge) {
				return new SciDBIslandMerge (this, addChild);
			} else {
				throw new IslandException("Unsupported SciDBIslandOperator Copy: "+this.getClass().toString());
			}
		} catch (JSQLParserException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}
	
	
	
	public void updateObjectAliases() {
		
		setObjectAliases(new HashSet<String>());
		if (this instanceof SciDBIslandScan && ((SciDBIslandScan)this).getArrayAlias() != null) {
			getObjectAliases().add(((SciDBIslandScan)this).getArrayAlias());
		} else if (this instanceof SciDBIslandJoin) {
			
			((SciDBIslandOperator) children.get(1)).updateObjectAliases();
			getObjectAliases().addAll(((SciDBIslandOperator) children.get(1)).getObjectAliases());
		} 

		if (children.size() != 0) {
			((SciDBIslandOperator) children.get(0)).updateObjectAliases();
			getObjectAliases().addAll(((SciDBIslandOperator) children.get(0)).getObjectAliases());
		}
	}
	
	
	
	// will likely get overridden
	@Override
	public String getTreeRepresentation(boolean isRoot) throws IslandException{
		throw new IslandException("Unimplemented: "+this.getClass().toString());
	}
	
	// half will be overriden
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException{
		System.out.printf("SciDBIslandOperator calling default getObjectToExpressionMappingForSignature; class: %s;\n", this.getClass().getSimpleName());
		return children.get(0).getObjectToExpressionMappingForSignature();
	}
	
	public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) {
		for (Operator o: children)
			((SciDBIslandOperator) o).removeCTEEntriesFromObjectToExpressionMapping(entry);
	}
	
	/**
	 * Serves only getObjectToExpressionMappingForSignature()
	 * @param e
	 * @param out
	 * @throws Exception 
	 */
	protected void addToOut(Expression e, Map<String, Set<String>> out, Map<String, String> aliasMapping) throws IslandException {
		
		while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
		SQLExpressionUtils.restoreTableNamesFromAliasForSignature(e, aliasMapping);
		
		
		Set<String> names = SQLExpressionUtils.getAllTableNamesForSignature(e, aliasMapping);
		for (String n : names) {
			
			if (out.get(n) == null) {
				Set<String> addition = new HashSet<>();
				addition.add(e.toString());
				out.put(n, addition);
			} else {
				out.get(n).add(e.toString());
			}
		}
	}
	
	public void updateOutSchema(Map<String, DataObjectAttribute> schema) throws JSQLParserException {
		Map<String, DataObjectAttribute> update = new HashMap<>();
		for (String s: schema.keySet()) {
			if (schema.get(s) instanceof SQLAttribute) update.put(new String(s), new SQLAttribute((SQLAttribute)schema.get(s)));
			else update.put(new String(s), new DataObjectAttribute(schema.get(s)));
		}
		this.outSchema = update;
	}

	@Override
	public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
		operatorQueryGenerator.visit(this);
	}

	public Set<String> getObjectAliases() {
		return objectAliases;
	}

	public void setObjectAliases(Set<String> objectAliases) {
		this.objectAliases = objectAliases;
	}

	@Override
	public List<Operator> getAllBlockers(){
		
		List<Operator> extraction = new ArrayList<>();
		
		if (isBlocking) {
			extraction.add(this);
			return extraction;
		}
		
		for (Operator c : children) {
			extraction.addAll(c.getAllBlockers());
		}
		
		return extraction;
	}


	@Override
	public Integer getPruneID() {
		if (isPruned) return pruneID;
		else return null;
	}
}

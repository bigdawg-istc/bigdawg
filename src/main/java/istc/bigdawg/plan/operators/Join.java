package istc.bigdawg.plan.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.plan.extract.CommonOutItem;
import istc.bigdawg.plan.extract.SQLOutItem;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.SelectUtils;


public class Join extends Operator {

	public enum JoinType  {Left, Natural, Right};
	
	private JoinType joinType = null;
	private String joinPredicate = null;
	private String joinFilter = null; 
	private List<String> aliases;
	private String joinPredicateOriginal = null; // TODO determine if this is useful for constructing new remainders 
	private String joinFilterOriginal = null; 
	
	protected Map<String, DataObjectAttribute> srcSchema;
	protected boolean joinPredicateUpdated = false;
	
	
	protected static int maxJoinSerial = 0;
	protected Integer joinID = null;
	
	
	
	// for SQL
	public Join (Map<String, String> parameters, List<String> output, Operator lhs, Operator rhs, SQLTableExpression supplement) throws Exception  {
		super(parameters, output, lhs, rhs, supplement);

		// mending non-canoncial ordering
		if (children.get(0) instanceof Scan && !(children.get(1) instanceof Scan)) {
			Operator child0 = children.get(1);
			Operator child1 = children.get(0);
			children.clear();
			children.add(child0);
			children.add(child1);
		}
		
		this.isBlocking = false;
		this.aliases = new ArrayList<>();
		
		maxJoinSerial++;
		this.joinID = maxJoinSerial;
	
		srcSchema = new LinkedHashMap<String, DataObjectAttribute>(lhs.outSchema);
		srcSchema.putAll(rhs.outSchema);
		
		for(int i = 0; i < output.size(); ++i) {
			String expr = output.get(i);
			
			SQLOutItem out = new SQLOutItem(expr, srcSchema, supplement);

			DataObjectAttribute attr = out.getAttribute();
			
//			attr.setExpression(rewriteComplextOutItem(attr.getExpressionString()));
			
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		
		// process join predicates and join filters
		// if hash join "Hash-Cond", merge join "Merge-Cond"
		for(String p : parameters.keySet()) 
			if(p.endsWith("Cond")) 
				joinPredicate = parameters.get(p);
		joinFilter = parameters.get("Join-Filter");
		if (joinFilter != null)  joinFilter = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(joinFilter);
		if (joinPredicate != null) joinPredicate = SQLExpressionUtils.removeExpressionDataTypeArtifactAndConvertLike(joinPredicate);
	
		if(joinFilter != null && joinPredicate != null && joinFilter.contains(joinPredicate)) { // remove duplicate
			String joinClause = "(" + joinPredicate + ") AND"; // canonical form
			if(joinFilter.contains(joinClause)) 			
				joinFilter = joinFilter.replace(joinClause, "");
			else {
				joinClause = " AND (" + joinPredicate + ")";
				joinFilter = joinFilter.replace(joinClause, "");			
			}
		} 
		
		inferJoinParameters();
		
		if (joinPredicate != null)
			joinPredicateOriginal 	= new String (joinPredicate);
		if (joinFilter != null)
			joinFilterOriginal 		= new String (joinFilter);
		
		for (Operator o : children) {
			if (o instanceof Aggregate) {
				((Aggregate)o).setSingledOutAggregate();
			}
		}
	}
    
	
	// for AFL
	public Join(Map<String, String> parameters, SciDBArray output, Operator lhs, Operator rhs) throws Exception  {
		super(parameters, output, lhs, rhs);

		maxJoinSerial++;
		this.joinID = maxJoinSerial;
		
		isBlocking = false;
		
		joinPredicate = parameters.get("Join-Predicate");
		aliases = Arrays.asList(parameters.get("Children-Aliases").split(" "));

		srcSchema = new LinkedHashMap<String, DataObjectAttribute>(lhs.outSchema);
		srcSchema.putAll(rhs.outSchema);
		
		// attributes
		for (String expr : output.getAttributes().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, output.getAttributes().get(expr), false, srcSchema);
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		
		// dimensions
		for (String expr : output.getDimensions().keySet()) {
			
			CommonOutItem out = new CommonOutItem(expr, "Dimension", true, srcSchema);
			DataObjectAttribute attr = out.getAttribute();
			String attrName = attr.getFullyQualifiedName();		
			outSchema.put(attrName, attr);
				
		}
		
		inferJoinParameters();
		
		if (joinPredicate != null)
			joinPredicateOriginal 	= new String (joinPredicate);
		if (joinFilter != null)
			joinFilterOriginal 		= new String (joinFilter);
	}
	
	// combine join ON clause with WHEREs that combine two tables
 	// if a predicate references data that is not public, move it to the filter
 	// collect equality predicates over public attributes in joinPredicate
 	// only supports AND in predicates, not OR or NOT
 	private void inferJoinParameters() throws Exception {
 		
 		if(joinFilter == null && joinPredicate == null) return;
 		if (joinPredicate != null && joinPredicate.length() > 0) { 
 			Expression jp = CCJSqlParserUtil.parseCondExpression(joinPredicate);
 			SQLExpressionUtils.removeExcessiveParentheses(jp);
 			joinPredicate = jp.toString(); 
 		}
 		if (joinFilter != null && joinFilter.length() > 0) {
 			Expression jf = CCJSqlParserUtil.parseCondExpression(joinFilter);
 			SQLExpressionUtils.removeExcessiveParentheses(jf);
 			joinFilter = jf.toString(); 
 		}
 	}
    
 	public Join (Operator o, boolean addChild) throws Exception {
		super(o, addChild);
		Join j = (Join) o;
		
		this.joinID = j.joinID;
		
		this.joinType = j.joinType;
		this.isCopy = j.isCopy;
		
		if (j.joinPredicate == null) this.joinPredicate = j.joinPredicate;
		else this.joinPredicate = new String(j.joinPredicate);
		
		if (j.joinFilter == null) this.joinFilter = j.joinFilter;
		else this.joinFilter = new String(j.joinFilter);

		this.srcSchema = new HashMap<>();
		for (String s : j.srcSchema.keySet()) {
			if (j.srcSchema.get(s) != null) 
				this.srcSchema.put(new String(s), new DataObjectAttribute(j.srcSchema.get(s)));
		}
		
		this.aliases = new ArrayList<>();
		if (j.aliases != null) {
			for (String a : j.aliases) {
				this.aliases.add(new String(a));
			}
		}
	}
	
	public Join(Operator child0, Operator child1, JoinType jt, String joinPred) throws JSQLParserException {
		this.isCTERoot = false; // TODO VERIFY
		this.isBlocking = false; 
		this.isPruned = false;
		this.isCopy = true;
		this.aliases = new ArrayList<>();
		this.complexOutItemFromProgeny = new LinkedHashMap<>();
		
		maxJoinSerial++;
		this.joinID = maxJoinSerial;
		 
		
		if (jt != null) this.joinType = jt;
		
		if (joinPred != null) {
			this.joinPredicate = joinPred;
			
		}
		
		this.isQueryRoot = true;
		
		this.dataObjects = new HashSet<>();
		this.joinReservedObjects = new HashSet<>();
		
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

    @Override
	public Select generateSQLStringDestOnly(Select dstStatement, boolean isSubTreeRoot, boolean stopAtJoin, Set<String> allowedScans) throws Exception {
		
    	if (!isSubTreeRoot && stopAtJoin) return generateSelectWithToken(getJoinToken());
//    	if (stopAtJoin == null) stopAtJoin = true;
    	
		if (isPruned) {
			return generateSelectWithToken(getPruneToken());
		}
    	
		allowedScans = this.getDataObjectAliasesOrNames().keySet();
		
    	Operator child0 = children.get(0);
    	Operator child1 = children.get(1);
    	
    	// constructing the ON expression
    	
    	Table t0 = new Table();
		Table t1 = new Table();
    	
    	if (joinPredicate != null) {
    		
    		joinPredicate = updateOnExpression(joinPredicate, child0, child1, t0, t1, true);
    		
    		joinPredicateUpdated = true;
    		// ^ ON predicate constructed
    		
//    		System.out.printf("\nupdated on: JF: %s; JP: %s\n\n\n", joinFilter, joinPredicate);
    		
    	} else {
        	Map<String, String> child0ObjectMap = child0.getDataObjectAliasesOrNames();
        	Map<String, String> child1ObjectMap = child1.getDataObjectAliasesOrNames();

        	String s, s2;
        	List<String> ses;
        	if ((ses = processLeftAndRightWithIndexCond(true)) != null) {
        		s = ses.get(0);
        		s2 = ses.get(1);
        		
        	} else if ((ses = processLeftAndRightWithIndexCond(false)) != null) {
        		s = ses.get(1);
        		s2 = ses.get(0);
        	} else {
        		if (joinFilter == null || joinFilter.isEmpty()) 
        			throw new Exception("Ses from Join gen dest only doesn't find match; joinFilter empty: "+joinFilter+"; "+joinPredicate);
        		try {
	        		List<Column> lc = SQLExpressionUtils.getAttributes(CCJSqlParserUtil.parseCondExpression(joinFilter));
	        		s = lc.get(0).getTable().getName();
	        		s2 = lc.get(1).getTable().getName();
        		} catch (Exception e) {
        			e.printStackTrace();
        			throw new Exception(String.format("Ses from Join gen dest only doesn't find match: %s; %s; %s;\n",child0.getChildrenIndexConds(), child1.getChildrenIndexConds(),joinFilter));
        		}
        	}
        	
        	// TODO MODIFIED
        	if (child0.isPruned()) {
        		t0.setName(child0.getPruneToken());
        	} else if (child0 instanceof Aggregate && stopAtJoin) {
        		t0.setName(((Aggregate)child0).getAggregateToken());
        	} else {
        		t0.setName(child0ObjectMap.get(s));
        		if (! s.equals(child0ObjectMap.get(s))) t0.setAlias(new Alias(s));
        	} 
        	
        	if (child1.isPruned()) {
        		t1.setName(child1.getPruneToken());
        	} else if (child1 instanceof Aggregate && stopAtJoin) {
        		t1.setName(((Aggregate)child1).getAggregateToken());
        	} else {
        		t1.setName(child1ObjectMap.get(s2));
        		if (! s2.equals(child1ObjectMap.get(s2))) t1.setAlias(new Alias(s2));
        	} 
        	
    	}

//		// for debugging
//    	Map<String, String> child0ObjectMap = child0.getDataObjectAliasesOrNames();
//    	Map<String, String> child1ObjectMap = child1.getDataObjectAliasesOrNames();
//    	System.out.printf("\nJF: %s; JP: %s\nchild0obj: %s\nchild1obj: %s\n\n", joinFilter, joinPredicate, 
//    			child0ObjectMap, child1ObjectMap);
    	
    	if (dstStatement == null && (child0.isPruned() || child0 instanceof Scan)) {

			// ensuring this is a left deep tree
			if (!(child1.isPruned() || child1 instanceof Scan)) 
				throw new Exception("child0 class: "+child0.getClass().toString()+"; child1 class: "+child1.getClass().toString());
			
			dstStatement = children.get(0).generateSQLStringDestOnly(null, false, stopAtJoin, allowedScans);
			if (t0.getAlias() != null) updateThisAndParentJoinReservedObjects(t0.getAlias().getName());
			else updateThisAndParentJoinReservedObjects(t0.getName());

    	} else if (child0 instanceof Aggregate && stopAtJoin) {
    		dstStatement = SelectUtils.buildSelectFromTable(new Table(((Aggregate)child0).getAggregateToken()));
    		List<SelectItem> sil = new ArrayList<>();
    		for (String s : child0.getOutSchema().keySet()){
    			sil.add(new SelectExpressionItem(new Column(new Table(((Aggregate)child0).getAggregateToken()), s)));
    		};
    		((PlainSelect)dstStatement.getSelectBody()).setSelectItems(sil);
		} else {
			dstStatement = child0.generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans); 
		}
    	
		
		// Resolve pruning and add join
    	if (child0.isPruned()) ((PlainSelect) dstStatement.getSelectBody()).setFromItem(t0);
    	addJSQLParserJoin(dstStatement, t1);
		
    	
    	if (child1 instanceof Aggregate && stopAtJoin) {
    		dstStatement = SelectUtils.buildSelectFromTable(t1);
    		List<SelectItem> sil = new ArrayList<>();
    		for (String s : child0.getOutSchema().keySet()){
    			sil.add(new SelectExpressionItem(new Column(t1, s)));
    		};
    		((PlainSelect)dstStatement.getSelectBody()).getSelectItems().addAll(sil);
		} else {
			dstStatement = child1.generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans); 
		}
    	
    	
//		dstStatement = child1.generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans); 
		
		if (joinFilter != null || joinPredicate != null) {

			String jf = joinFilter;
			if (jf == null || jf.length() == 0) jf = joinPredicate;
			else if (joinPredicate != null && joinPredicate.length() > 0) jf = jf + " AND " + joinPredicate; 
			
			Expression e = CCJSqlParserUtil.parseCondExpression(jf);
			List<Column> filterRelatedTablesExpr = SQLExpressionUtils.getAttributes(e); 
			List<String> filterRelatedTables = new ArrayList<>();
			for (Column c : filterRelatedTablesExpr) {
				filterRelatedTables.add(c.getTable().getFullyQualifiedName());
				if (c.getTable().getAlias() != null) filterRelatedTables.add(c.getTable().getAlias().getName());
			}
			
			List<Operator> treeWalker = new ArrayList<>(this.children);
			List<Operator> nextGen;

			this.updateObjectAliases(true);
			
			while (!treeWalker.isEmpty()) {
				nextGen = new ArrayList<>();
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						Set<String> aliasesAndNames = new HashSet<>(o.objectAliases);
						aliasesAndNames.addAll(o.dataObjects);
						Set<String> duplicate = new HashSet<>(aliasesAndNames);
						if (aliasesAndNames.removeAll(filterRelatedTables)) 
							SQLExpressionUtils.renameAttributes(e, duplicate, null, o.getPruneToken());
					} else 
						nextGen.addAll(o.children);
				}
				treeWalker = nextGen;
			}
			
			jf = e.toString();
			
			String currentWhere = jf;// StringUtils.join(filterSet, " AND ");
			
			if (!currentWhere.isEmpty()) {
				Expression where = 	CCJSqlParserUtil.parseCondExpression(currentWhere);
				PlainSelect ps = ((PlainSelect) dstStatement.getSelectBody());
				if (ps.getWhere() == null) ps.setWhere(where);
				else ps.setWhere(new AndExpression(where, ps.getWhere()));
			}
		}
		
//		System.out.println("\n-- Join: "+dstStatement.toString()+"\n");
		
		return dstStatement;

	}
    
    private List<String> processLeftAndRightWithIndexCond(boolean zeroFirst) throws Exception {
    	
    	Map<String, Expression> child0Cond;
    	Map<String, Expression> child1Cond;

    	if (zeroFirst) {
    		child0Cond = children.get(0).getChildrenIndexConds();
    		child1Cond = children.get(1).getChildrenIndexConds();
    	} else {
    		child0Cond = children.get(1).getChildrenIndexConds();
    		child1Cond = children.get(0).getChildrenIndexConds();
    	}
    	
		for (String s : child0Cond.keySet()) {
			if (child0Cond.get(s) == null ) continue;
			List<Column> ls = SQLExpressionUtils.getAttributes(child0Cond.get(s));
			for (Column c : ls) {
				String s2 = c.getTable().getName();
				if (child1Cond.containsKey(s2)) {
					
//					// t0 gets s; t1 gets s2
//					if (! s.equals(child0ObjectMap.get(s))) t0.setAlias(new Alias(s));
//		        	t0.setName(child0ObjectMap.get(s));
//		        	
//		        	if (! s2.equals(child1ObjectMap.get(s2))) t1.setAlias(new Alias(s2));
//		        	t1.setName(child1ObjectMap.get(s2));
					
					List<String> ret = new ArrayList<>();
					ret.add(s);
					ret.add(s2);
					
					return ret;
				}
			}
		}
		return null;
    }
    
    
    private void updateThisAndParentJoinReservedObjects(String name) {
    	Operator o = this;
    	o.joinReservedObjects.add(name);
    	while (o.parent != null) {
    		o.parent.joinReservedObjects.add(name);
    		o = o.parent;
    	}
    }
    
    private boolean replaceTableNameWithPruneName(Operator child, Expression e, Table t, List<String> itemsSet) throws Exception {
		if (child.isPruned()) {
			// does child have any of those names? 
			Set<String> names = new HashSet<>(child.getDataObjectNames());
			if (child.objectAliases == null) child.updateObjectAliases(true);
			names.addAll(child.objectAliases);
			names.retainAll(itemsSet);
			if (names.size() > 0) {
				SQLExpressionUtils.renameAttributes(e, names, null, child.getPruneToken());
				t.setName(child.getPruneToken());
				return true;
			} else 
				return false;
		} else if (child instanceof Aggregate && ((Aggregate)child).aggregateID != null) {
			// does child have any of those names? 
			Set<String> names = new HashSet<>(child.getDataObjectAliasesOrNames().keySet());
//			names.addAll(child.objectAliases);
			
			names.retainAll(itemsSet);
			if (names.size() > 0) {
				SQLExpressionUtils.renameAttributes(e, names, null, ((Aggregate)child).getAggregateToken());
				t.setName(((Aggregate)child).getAggregateToken());
				return true;
			} else 
				return false;
		}else {
			boolean ret = false;
			for (Operator o : child.getChildren()) {
				ret = ret || replaceTableNameWithPruneName(o, e, t, itemsSet);
			}
			return ret;
		}
	}
    
    private boolean findAndGetTableName(Operator child, Table t, List<String> itemsSet) throws Exception {
    	
		Set<String> names = new HashSet<>(child.getDataObjectNames());
		child.updateObjectAliases(true);
		names.addAll(child.objectAliases);
		names.retainAll(itemsSet);
		if (names.size() > 0) {
			if (child instanceof Scan) {
				t.setName(((Scan)child).getTable().toString());
			} else if (child.getChildren().size() > 0) {
				return findAndGetTableName(child.getChildren().get(0), t, itemsSet);
			}
			return false;
		} else {
			boolean ret = false;
			for (Operator o : child.getChildren()) {
				ret = ret || findAndGetTableName(o, t, itemsSet);
			}
			return ret;
		}    	
    }
    
    public String updateOnExpression(String joinPred, Operator child0, Operator child1, Table t0, Table t1, boolean update) throws Exception {
    	
    	if ((!update) && joinPredicateUpdated)
    		return joinPredicate;
    	
    	Expression expr = CCJSqlParserUtil.parseCondExpression(joinPred);
		List<String> itemsSet = SQLExpressionUtils.getColumnTableNamesInAllForms(expr);
		
		if (!replaceTableNameWithPruneName(child0, expr, t0, itemsSet))
			findAndGetTableName(child0, t0, itemsSet);
		if (!replaceTableNameWithPruneName(child1, expr, t1, itemsSet))
			findAndGetTableName(child1, t1, itemsSet);
		
		return expr.toString();
	}
    
    
    public String toString() {
    		return "Joining " + children.get(0).toString() + " x " + children.get(1).toString() 
    				+ " type " + joinType + " predicates " + joinPredicate + " filters " + joinFilter;
    }
    
	@Override
	public String generateAFLString(int recursionLevel) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("cross_join(");
		
		if (children.get(0).isPruned())
			sb.append(children.get(0).getPruneToken());
		else 
			sb.append(children.get(0).generateAFLString(recursionLevel+1));
		
		if (!this.aliases.isEmpty()) 
			sb.append(" as ").append(aliases.get(0));
		sb.append(", ");
		
		if (children.get(1).isPruned())
			sb.append(children.get(1).getPruneToken());
		else 
			sb.append(children.get(1).generateAFLString(recursionLevel+1));
		
		if (!this.aliases.isEmpty()) sb.append(" as ").append(aliases.get(1));
		
		if (joinPredicate != null) {
			sb.append(", ");
			sb.append(joinFilter.replaceAll("[<>= ()]+", " ").replace("\\s+", ", "));
		}
		
		sb.append(')');
		return sb.toString();
	}
	
	public String getJoinPredicateOriginal() {
		return joinPredicateOriginal;
	}
	public String getJoinFilterOriginal() {
		return joinFilterOriginal;
	}
	
	private void addJSQLParserJoin(Select dstStatement, Table t) {
		net.sf.jsqlparser.statement.select.Join newJ = new net.sf.jsqlparser.statement.select.Join();
    	newJ.setRightItem(t);
    	newJ.setSimple(true);
    	if (((PlainSelect) dstStatement.getSelectBody()).getJoins() == null)
    		((PlainSelect) dstStatement.getSelectBody()).setJoins(new ArrayList<>());
    	((PlainSelect) dstStatement.getSelectBody()).getJoins().add(newJ);
	}
	
	public String getCurrentJoinPredicate(){
		return joinPredicate;
	};
	
	@Override
	public String getTreeRepresentation(boolean isRoot) throws Exception{
		if (isPruned() && (!isRoot)) return "{PRUNED}";
		else {
			StringBuilder sb = new StringBuilder();
			sb.append("{join").append(children.get(0).getTreeRepresentation(false)).append(children.get(1).getTreeRepresentation(false));

//			if (joinFilter != null && !joinFilter.isEmpty()) {
//				Expression e = CCJSqlParserUtil.parseCondExpression(joinFilter);
//				SQLExpressionUtils.removeExcessiveParentheses(e);
//				sb.append(SQLExpressionUtils.parseCondForTree(e));
//			}
//			
//			List<Operator> treeWalker = this.getChildren();
//			while (!treeWalker.isEmpty()) {
//				List<Operator> nextgen = new ArrayList<>();
//				for (Operator o : treeWalker) {
//					if (o instanceof Join) continue;
//					if (o instanceof Scan && ((Scan) o).indexCond != null) {
//						sb.append(SQLExpressionUtils.parseCondForTree(((Scan)o).indexCond));
//					} else {
//						nextgen.addAll(o.getChildren());
//					}
//				}
//				treeWalker = nextgen;
//			}
			sb.append('}');
			return sb.toString();
		}
	}
	
	
	public String getJoinToken() {
		
		if (joinID == null) {
			maxJoinSerial ++;
			joinID = maxJoinSerial;
		}
		
		return "BIGDAWGJOINTOKEN_"+joinID;
	}
	
	
	
	/**
	 * This one only supports equal sign and Column expressions
	 * @return
	 * @throws Exception
	 */
	public List<String> getJoinPredicateObjectsForBinaryExecutionNode() throws Exception {
		
		List<String> ret = new ArrayList<String>();
		
		if (joinPredicate == null || joinPredicate.length() == 0) {
			
			Expression extraction = null;
			Column leftColumn = null;
			Column rightColumn = null;
			
			
			List<String> ses = processLeftAndRightWithIndexCond(true);
			String s, s2; 
			if (ses != null) {
				s = ses.get(0);
				s2 = ses.get(1);
				extraction = this.getChildren().get(0).getChildrenIndexConds().get(s);
			} else {
				ses = processLeftAndRightWithIndexCond(false);
				if (ses == null) return ret;
				s = ses.get(1);
				s2 = ses.get(0);
				extraction = this.getChildren().get(1).getChildrenIndexConds().get(s2);
			}
			
			List<Column> ls = SQLExpressionUtils.getAttributes(extraction);
			for (Column c2 : ls) if (c2.getTable().getName().equals(s)) {leftColumn = c2; break;}
			for (Column c2 : ls) if (c2.getTable().getName().equals(s2)) {rightColumn = c2; break;}
			
			while (extraction instanceof Parenthesis) extraction = ((Parenthesis)extraction).getExpression();
			ret.add(SQLExpressionUtils.getBinaryExpressionOperatorToken(extraction));
			ret.add(String.format("{%s, %s}", leftColumn.getTable().getFullyQualifiedName(),leftColumn.getColumnName()));
			ret.add(String.format("{%s, %s}", rightColumn.getTable().getFullyQualifiedName(),rightColumn.getColumnName()));
			
        	return ret;
		}
			
		
		
		Set<String> leftChildObjects = this.getChildren().get(0).getDataObjectNames();

//		System.out.println("---> Left Child objects: "+leftChildObjects.toString());
//		System.out.println("---> Right Child objects: "+rightChildObjects.toString());
//		System.out.println("---> joinPredicate: "+joinPredicate);
		
		Expression e = CCJSqlParserUtil.parseCondExpression(joinPredicate);
		
		
		while (e instanceof Parenthesis)
			e = ((Parenthesis)e).getExpression();
		
		
		ret.add(SQLExpressionUtils.getBinaryExpressionOperatorToken(e));
		
		
		// TODO SUPPORT MORE THAN COLUMN?
		
		Column left = (Column)((EqualsTo)e).getLeftExpression();
		Column right = (Column)((EqualsTo)e).getRightExpression();
		
		if (leftChildObjects.contains(left.getTable().getName()) || leftChildObjects.contains(left.getTable().getFullyQualifiedName())) {
			ret.add(String.format("{%s, %s}", left.getTable().getFullyQualifiedName(),left.getColumnName()));
			ret.add(String.format("{%s, %s}", right.getTable().getFullyQualifiedName(),right.getColumnName()));
		} else {
			ret.add(String.format("{%s, %s}", right.getTable().getFullyQualifiedName(),right.getColumnName()));
			ret.add(String.format("{%s, %s}", left.getTable().getFullyQualifiedName(),left.getColumnName()));
		}
		
//		System.out.println("---> joinPredicate ret: "+ret.toString()+"\n\n\n");
		
		return ret;
	}
	
	@Override
	public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws Exception{
		
		Operator parent = this;
		while (!parent.isBlocking && parent.parent != null ) parent = parent.parent;
		Map<String, String> aliasMapping = this.getDataObjectAliasesOrNames();
		
		Map<String, Set<String>> out = children.get(0).getObjectToExpressionMappingForSignature();
		Map<String, Set<String>> temp = children.get(1).getObjectToExpressionMappingForSignature();
		
		for (String s : temp.keySet()) {
			if (out.containsKey(s)) 
				out.get(s).addAll(temp.get(s));
			else 
				out.put(s, temp.get(s));
		}

		// joinFilter
		Expression e;
		if (joinFilter != null) { 
			e = CCJSqlParserUtil.parseCondExpression(joinFilter);
			if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e))
				addToOut(e, out, aliasMapping);
		}
		
		// joinPredicate
		if (joinPredicate != null) { 
			e = CCJSqlParserUtil.parseCondExpression(joinPredicate);
			if (!SQLExpressionUtils.containsArtificiallyConstructedTables(e))
				addToOut(e, out, aliasMapping);
		}
		
		return out;
	}
	
	@Override
	protected Map<String, Expression> getChildrenIndexConds() throws Exception {
		Map<String, Expression> left = this.getChildren().get(0).getChildrenIndexConds();
		Map<String, Expression> right = this.getChildren().get(1).getChildrenIndexConds();
		
		boolean found = false; 
		for (String s : left.keySet()) {
			if (left.get(s) == null ) continue;
			List<Column> ls = SQLExpressionUtils.getAttributes(left.get(s));
			for (Column c : ls) {
				String s2 = c.getTable().getName();
				if (right.containsKey(s2)) {
					left.replace(s, null);
					found = true;
					break;
				}
			}
			if (found) break;
		}
		
		found = false; 
		for (String s : right.keySet()) {
			if (right.get(s) == null ) continue;
			List<Column> ls = SQLExpressionUtils.getAttributes(right.get(s));
			for (Column c : ls) {
				String s2 = c.getTable().getName();
				if (left.containsKey(s2)) {
					right.replace(s, null);
					found = true;
					break;
				}
			}
			if (found) break;
		}
		
		left.putAll(right);
		return left;
	}
	
	@Override
	public Expression resolveAggregatesInFilter(String e, boolean goParent, Operator lastHopOp, Set<String> names, StringBuilder sb) throws Exception {
		
		Expression exp = null;
		if (parent != null && lastHopOp != parent && (exp = parent.resolveAggregatesInFilter(e, true, this, names, sb)) != null) 
			return exp;
		for (Operator o : children) {
			if (goParent && o == lastHopOp) continue;
			if ((exp = o.resolveAggregatesInFilter(e, false, this, names, sb)) != null) return exp;
		}
		return exp;
		
	} 
	
	@Override
	public void seekScanAndProcessAggregateInFilter() throws Exception {
		
		if (joinFilter != null) {
			joinFilter = processFilterForAggregateEntry(joinFilter);
		}
		
		if (joinPredicate != null) {
			joinPredicate = processFilterForAggregateEntry(joinPredicate);
		}
		
		super.seekScanAndProcessAggregateInFilter();
	}
	
	private String processFilterForAggregateEntry(String s) throws Exception {
		
		
		
		Expression e = CCJSqlParserUtil.parseCondExpression(s);
		
		if (!SQLExpressionUtils.isFunctionPresentInCondExpression(e)) return s;
		
		Expression left = SQLExpressionUtils.getOneSideOfBinaryCondExpression(e, true);
		while (left instanceof Parenthesis) left = ((Parenthesis) left).getExpression();
		
		StringBuilder sb = new StringBuilder();
		Set<String> names = new HashSet<>();
		Expression result = resolveAggregatesInFilter(left.toString(), true, this, names, sb);
		if (result != null) {
			SQLExpressionUtils.setOneSideOfBinaryCondExpression(result, e, true);
		}
		
		Expression right = SQLExpressionUtils.getOneSideOfBinaryCondExpression(e, false);
		while (right instanceof Parenthesis) right = ((Parenthesis) right).getExpression();
		result = resolveAggregatesInFilter(right.toString(), true, this, names, sb);
		if (result != null) {
			SQLExpressionUtils.setOneSideOfBinaryCondExpression(result, e, false);
		}
		
		return e.toString();
	}
};

package istc.bigdawg.plan.generators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import istc.bigdawg.extract.logical.SQLExpressionHandler;
import istc.bigdawg.plan.operators.Aggregate;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Distinct;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Limit;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.Scan;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.plan.operators.Sort;
import istc.bigdawg.plan.operators.WindowAggregate;
import istc.bigdawg.schema.DataObjectAttribute;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.SelectUtils;

public class SQLQueryGenerator implements OperatorVisitor {

	Select srcStatement = null;
	Select dstStatement = null;
	boolean stopAtJoin = false;
	boolean isRoot = true;
	Operator root = null;
	Set<String> allowedScans = new HashSet<>();
	
	@Override
	public void configure(boolean isRoot, boolean stopAtJoin) {
		this.stopAtJoin = stopAtJoin;
		this.isRoot = isRoot;
	}
	
	@Override
	public void reset(boolean isRoot, boolean stopAtJoin) {
		srcStatement = null;
		dstStatement = null;
		this.stopAtJoin = stopAtJoin;
		this.isRoot = isRoot;
		root = null;
		allowedScans = new HashSet<>();
	}
	
	public void saveRoot(Operator o) {
		if (!this.isRoot) return;
		this.root = o;
		this.isRoot = false;
		dstStatement = null;
	}
	
	public void setSrcStatement(Select srcStatement) {
		this.srcStatement = srcStatement;
	}
	
	public void setAllowedScan(Set<String> allowedScans) {
		if (allowedScans != null) this.allowedScans = new HashSet<>(allowedScans);
	}

	@Override
	public String generateStatementString() throws Exception {
		postprocessSQLStatement(root);
		return dstStatement.toString();
	}
	
	@Override
	public void visit(Operator operator) throws Exception{
		saveRoot(operator);
		for (Operator o : operator.getChildren())
			o.accept(this);
		
	}

	@Override
	public void visit(Join join) throws Exception {
		if (root == null) root = join; 
		if (!isRoot && join.isPruned()) {
			dstStatement = generateSelectWithToken(join, join.getPruneToken());
			return ;
		}
		if (!isRoot && stopAtJoin) {
			dstStatement = generateSelectWithToken(join, join.getJoinToken());
			return ;
		}
		saveRoot(join);
    	Operator child0 = join.getChildren().get(0);
    	Operator child1 = join.getChildren().get(1);
    	
    	// constructing the ON expression
    	
    	Table t0 = new Table();
		Table t1 = new Table();
		String discoveredAggregateFilter = null;
		List<String> discoveredJoinPredicate = new ArrayList<>();
		StringBuilder jf = new StringBuilder();
		
    	if (join.getOriginalJoinPredicate() != null) {
    		jf.append(updateOnExpression(join.getOriginalJoinPredicate(), child0, child1, t0, t1, true));
    	} else {
        	Map<String, String> child0ObjectMap = child0.getDataObjectAliasesOrNames();
        	Map<String, String> child1ObjectMap = child1.getDataObjectAliasesOrNames();

        	String s, s2;
        	List<String> ses;
        	if ((ses = processLeftAndRightWithIndexCond(join, true, discoveredJoinPredicate)) != null) {
        		s = ses.get(0);
        		s2 = ses.get(1);
        		
        	} else if ((ses = processLeftAndRightWithIndexCond(join, false, discoveredJoinPredicate)) != null) {
        		s = ses.get(1);
        		s2 = ses.get(0);
        	} else {
        		
        		if (stopAtJoin) {
	        		Operator leftChild = join.getChildren().get(0);
	        		String pred;
	        		while (!(leftChild instanceof Join) && leftChild.getChildren().size() > 0) leftChild = leftChild.getChildren().get(0);
	        			
	        		
	        		if (leftChild.getChildren().size() > 0 && (pred = ((Join)leftChild).getOriginalJoinPredicate()) != null) {
	        			
	        			System.out.printf("::::::: Pred left: %s; \n", pred);
	        			
	        			Expression e = CCJSqlParserUtil.parseCondExpression(pred);
	        			List<String> es = SQLExpressionUtils.getAttributes(e).stream().map(a -> a.getTable().getFullyQualifiedName()).collect(Collectors.toList());
	        			if ((new HashSet<>( child1ObjectMap.keySet())).removeAll(es)) {
	        				Expression exp = CCJSqlParserUtil.parseCondExpression(pred);
	        				SQLExpressionUtils.renameAttributes(exp, child1ObjectMap.keySet(), null, ((Join)leftChild).getJoinToken());
	        				discoveredJoinPredicate.add(exp.toString());
	        			}
	        		} 
	        		
	        		
	        		Operator rightChild = join.getChildren().get(1);
	        		while (!(rightChild instanceof Join) && rightChild.getChildren().size() > 0) rightChild = rightChild.getChildren().get(0);
	        		if ( rightChild.getChildren().size() > 0 && (pred = ((Join)rightChild).getOriginalJoinPredicate()) != null) {
	        			System.out.printf("::::::: Pred right: %s; \n", pred);
	        			Expression e = CCJSqlParserUtil.parseCondExpression(pred);
	        			List<String> es = SQLExpressionUtils.getAttributes(e).stream().map(a -> a.getTable().getFullyQualifiedName()).collect(Collectors.toList());
	        			if ((new HashSet<>( child0ObjectMap.keySet())).removeAll(es)) {
	        				Expression exp = CCJSqlParserUtil.parseCondExpression(pred);
	        				SQLExpressionUtils.renameAttributes(exp, child0ObjectMap.keySet(), null, ((Join)rightChild).getJoinToken());
	        				discoveredJoinPredicate.add(exp.toString());
	        			}
	        		}
        		}
        		
        		
        		
        		if (join.getOriginalJoinFilter() == null || join.getOriginalJoinFilter().isEmpty())  {
        			s = child0ObjectMap.keySet().iterator().next();
        			s2 = child1ObjectMap.keySet().iterator().next();
        		} else {
        			try {
    	        		List<Column> lc = SQLExpressionUtils.getAttributes(CCJSqlParserUtil.parseCondExpression(join.getOriginalJoinFilter()));
    	        		s = lc.get(0).getTable().getName();
    	        		s2 = lc.get(1).getTable().getName();
            		} catch (Exception e) {
            			e.printStackTrace();
            			throw new Exception(String.format("Ses from Join gen dest only doesn't find match: %s; %s; %s;\n"
            					,child0.getChildrenPredicates(), child1.getChildrenPredicates(), join.getOriginalJoinFilter()));
            		}
        		}
        	}
        	
//        	System.out.printf("--->> join, discoveredJoinPredicate: %s; joinPredicate: %s;\n"
//        			, discoveredJoinPredicate, join.getOriginalJoinPredicate());
        	
        	boolean anyPruned = join.isAnyProgenyPruned();
        	
        	if (child0.isPruned()) {
        		t0.setName(child0.getPruneToken());
        	} else if (child0 instanceof Aggregate && stopAtJoin) {
        		t0.setName(((Aggregate)child0).getAggregateToken());
        	} else if (child0 instanceof Join && anyPruned) {
        		t0.setName(((Join)child0).getJoinToken());
        	} else {
        		t0.setName(child0ObjectMap.get(s));
        		if (! s.equals(child0ObjectMap.get(s))) t0.setAlias(new Alias(s));
        	} 

        	if (child1.isPruned()) {
        		t1.setName(child1.getPruneToken());
        	} else if (child1 instanceof Aggregate && stopAtJoin) {
        		t1.setName(((Aggregate)child1).getAggregateToken());
        	} else if (child1 instanceof Join && anyPruned) {
        		t1.setName(((Join)child1).getJoinToken());
        	} else {
        		t1.setName(child1ObjectMap.get(s2));
        		if (! s2.equals(child1ObjectMap.get(s2))) t1.setAlias(new Alias(s2));
        	} 
    	}
    	
    	if (dstStatement == null && (child0.isPruned() || child0 instanceof Scan)) {

			// ensuring this is a left deep tree
			if (!(child1.isPruned() || child1 instanceof Scan)) 
				throw new Exception("child0 class: "+child0.getClass().getSimpleName().toString()+"; child1 class: "+child1.getClass().getSimpleName().toString());
			
			child0.accept(this);
    	} else if (child0 instanceof Aggregate && stopAtJoin) {
    		dstStatement = SelectUtils.buildSelectFromTable(new Table(((Aggregate)child0).getAggregateToken()));
    		List<SelectItem> sil = new ArrayList<>();
    		for (String s : child0.getOutSchema().keySet()){
    			sil.add(new SelectExpressionItem(new Column(new Table(((Aggregate)child0).getAggregateToken()), s)));
    		};
    		((PlainSelect)dstStatement.getSelectBody()).setSelectItems(sil);
		} else {
			child0.accept(this);
		}
		// Resolve pruning and add join
    	if (child0.isPruned()) ((PlainSelect) dstStatement.getSelectBody()).setFromItem(t0);
    	addJSQLParserJoin(dstStatement, t1);
		
    	
    	if (child1 instanceof Aggregate && stopAtJoin) {
    		List<SelectItem> sil = new ArrayList<>();
    		for (String s : child0.getOutSchema().keySet()){
    			sil.add(new SelectExpressionItem(new Column(t1, s)));
    		};
    		PlainSelect  ps = ((PlainSelect)dstStatement.getSelectBody());
    		ps.getSelectItems().addAll(sil);
    		
    		// need to go fetch a potential having filter
    		List<Operator> treeWalker = new ArrayList<>(join.getChildren());
			List<Operator> nextGen;
			boolean found = false;

			while (!treeWalker.isEmpty()) {
				nextGen = new ArrayList<>();
				for (Operator o : treeWalker) {
					if (o instanceof Scan) {
						Expression f = ((Scan) o).getFilterExpression();
						if (f != null && ((Scan)o).isHasFunctionInFilterExpression()) {
							populateComplexOutItem(join, false);
							Expression e = CCJSqlParserUtil.parseCondExpression(rewriteComplextOutItem(join, f));
							Set<String> aliases = child1.getDataObjectAliasesOrNames().keySet();
							SQLExpressionUtils.renameAttributes(e, aliases, aliases, ((Aggregate)child1).getAggregateToken());
							discoveredAggregateFilter = e.toString();
							found = true;
							break;
						}
					} else 
						nextGen.addAll(o.getChildren());
				}
				if (found) break;
				treeWalker = nextGen;
			}
    		
		} else {
			child1.accept(this); 
		}
    	
    	
		// WHERE setup
		Expression w = ((PlainSelect) dstStatement.getSelectBody()).getWhere();
		
		if (join.getOriginalJoinFilter() != null || join.getOriginalJoinPredicate() != null) {
			
			if (jf.length() == 0 && join.getOriginalJoinPredicate() != null && join.getOriginalJoinPredicate().length() > 0) 
				jf.append(updatePruneTokensForOnExpression(join, join.getOriginalJoinPredicate()));
			
			if (join.getOriginalJoinFilter() != null && join.getOriginalJoinFilter().length() > 0) {
				if (jf.length() > 0) jf.append(" AND ");
				jf.append(updatePruneTokensForOnExpression(join, join.getOriginalJoinFilter()));
			}
		}

		if (w != null) addToJoinFilter(w.toString(), jf);
		addToJoinFilter(discoveredAggregateFilter, jf);
		if (!discoveredJoinPredicate.isEmpty()) for (String s : discoveredJoinPredicate) addToJoinFilter(s, jf); // there should be only one entry
		
//		System.out.printf("--->>>> join getRelevantFilterSections: \n\texpr: %s;\n\tjf: %s;\n\tleft: %s,\n\tright: %s\n", 
//				SQLExpressionUtils.getRelevantFilterSections(CCJSqlParserUtil.parseCondExpression(jf), 
//						child0.getDataObjectAliasesOrNames(), 
//						child1.getDataObjectAliasesOrNames()),
//				jf ,
//				child0.getDataObjectAliasesOrNames().keySet(), 
//				child1.getDataObjectAliasesOrNames().keySet());
		
		String estring = null;
		// WHERE UPDATE
		
//		System.out.printf("----> jf: %s\njoinPred: %s\njoinFilter: %s\n\n", jf, join.getOriginalJoinPredicate(), join.getOriginalJoinFilter());
		
		if (jf.length() > 0 && !((estring = SQLExpressionUtils.getRelevantFilterSections(
				CCJSqlParserUtil.parseCondExpression(jf.toString()), 
				child0.getDataObjectAliasesOrNames(), 
				child1.getDataObjectAliasesOrNames())).isEmpty())) {
			
//			System.out.printf("--> estring: %s\n", estring);
			
			Expression e = CCJSqlParserUtil.parseCondExpression(estring);
			
			List<Column> filterRelatedTablesExpr = SQLExpressionUtils.getAttributes(e); 
			List<String> filterRelatedTables = new ArrayList<>();
			for (Column c : filterRelatedTablesExpr) {
				filterRelatedTables.add(c.getTable().getFullyQualifiedName());
				if (c.getTable().getAlias() != null) filterRelatedTables.add(c.getTable().getAlias().getName());
			}
			
			List<Operator> treeWalker = new ArrayList<>(join.getChildren());
			List<Operator> nextGen;

			join.updateObjectAliases();
			
			while (!treeWalker.isEmpty()) {
				nextGen = new ArrayList<>();
				for (Operator o : treeWalker) {
					if (o.isPruned()) {
						Set<String> aliasesAndNames = new HashSet<>(o.getObjectAliases());
						aliasesAndNames.addAll(o.getDataObjectAliasesOrNames().keySet());
						Set<String> duplicate = new HashSet<>(aliasesAndNames);
						if (aliasesAndNames.removeAll(filterRelatedTables)) 
							SQLExpressionUtils.renameAttributes(e, duplicate, null, o.getPruneToken());
					} else 
						nextGen.addAll(o.getChildren());
				}
				treeWalker = nextGen;
			}
			
			((PlainSelect) dstStatement.getSelectBody()).setWhere(CCJSqlParserUtil.parseCondExpression(e.toString()));
			
		}
		
	}

	@Override
	public void visit(Sort sort) throws Exception{
		saveRoot(sort);
		sort.getChildren().get(0).accept(this);
//		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans);

		if (sort.getChildren().get(0) instanceof Join) {
			PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
			ps.getSelectItems().clear();
			for (String alias: sort.getOutSchema().keySet()) {
				Expression e = CCJSqlParserUtil.parseExpression(sort.getOutSchema().get(alias).getExpressionString());
				SelectItem s = new SelectExpressionItem(e);
				if (!(e instanceof Column)) {
					((SelectExpressionItem)s).setAlias(new Alias(alias));
				}
				ps.addSelectItems(s);
			}
		}
		
		if(!sort.isWinAgg()) {
			((PlainSelect) dstStatement.getSelectBody()).setOrderByElements(sort.updateOrderByElements());
		}

	}

	@Override
	public void visit(Distinct distinct) throws Exception {
		saveRoot(distinct);
//		dstStatement = children.get(0).generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans);
		distinct.getChildren().get(0).accept(this);

		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
		if (distinct.getChildren().get(0) instanceof Join) {
			ps.getSelectItems().clear();
			for (String alias: distinct.getOutSchema().keySet()) {
				Expression e = CCJSqlParserUtil.parseExpression(distinct.getOutSchema().get(alias).getExpressionString());
				SelectItem s = new SelectExpressionItem(e);
				if (!(e instanceof Column)) {
					((SelectExpressionItem)s).setAlias(new Alias(alias));
				}
				ps.addSelectItems(s);
			}
		}
		
		ps.setDistinct(new net.sf.jsqlparser.statement.select.Distinct());
	}

	@Override
	public void visit(Scan scan) throws Exception {
		boolean rootStatus = isRoot;
		saveRoot(scan);
		if(dstStatement == null) {
			dstStatement = SelectUtils.buildSelectFromTable(scan.getTable());
		}
		
		if (scan.getFilterExpression() != null && (!scan.isPruned() || rootStatus) && !scan.isHasFunctionInFilterExpression()) { // FilterExpression with function is pulled
			
			Expression fe = CCJSqlParserUtil.parseCondExpression(scan.getFilterExpression().toString());
			
			List<Column> cs = SQLExpressionUtils.getAttributes(fe);
			List<String> ss = new ArrayList<>();
			for (Column c : cs)  ss.add(c.getTable().getName());
			ss.remove(scan.getSrcTable());
			if (scan.getTableAlias() != null) ss.remove(scan.getTableAlias());
			ss.removeAll(allowedScans);
			
			if (ss.isEmpty()) {
			
				PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
				
				Expression e = null; 
				if(ps.getWhere() != null) {
					e = new AndExpression(ps.getWhere(), fe);
				} else 
					e = fe;
				
				if ( e != null) e = CCJSqlParserUtil.parseCondExpression(e.toString());
				
				try {
					ps.setWhere(e);
				} catch (Exception ex) {
					System.out.println("filterSet exception: "+fe.toString());
				}
			}
		}
		
	}

	@Override
	public void visit(CommonSQLTableExpressionScan cte) throws Exception {
		saveRoot(cte);
//		dstStatement = super.generateSQLStringDestOnly(dstStatement, isSubTreeRoot, stopAtJoin, allowedScans);
		visit((Scan) cte);
		
		List<WithItem> withs = dstStatement.getWithItemsList();
		
		boolean found = false;
		if(withs != null && !withs.isEmpty()) {
			for(WithItem w : withs) {
				if(w.getName().equals(cte.getCteName())) {
					found = true;
				}
			}
		}
		
		
		// insert WithItem at the beginning of list
		if(!found) {
			List<WithItem> dstWiths = new ArrayList<WithItem>();
			
			if(dstStatement.getWithItemsList() != null && !(dstStatement.getWithItemsList().isEmpty())) {
				dstWiths.addAll(dstStatement.getWithItemsList());
			}
			dstWiths.add(cte.getWith());

			
			dstStatement.setWithItemsList(dstWiths);
			// recurse if child references any additional CTEs
			// create new dst statement for child and grab its select body
			
			Set<String> originalAllowedScans = allowedScans;
			allowedScans = new HashSet<>();
			
			cte.getSourceStatement().accept(this);//.generateSQLStringDestOnly(null, true, stopAtJoin, new HashSet<>());
			Select dstPrime = dstStatement;
			
			List<WithItem> dstWithsPrime = dstPrime.getWithItemsList();
			if(dstWithsPrime != null) {
				for(int i = 0; i < dstWithsPrime.size(); ++i) {
					if(dstWiths.contains(dstWithsPrime.get(i))) {
						dstWithsPrime.remove(i);
						i = 0; // restart
					}
				}
				if(!dstWithsPrime.isEmpty()) {
					dstWithsPrime.addAll(dstWiths);
					dstStatement.setWithItemsList(dstWithsPrime);
				}
			} // end "have child cte" check
			allowedScans = originalAllowedScans;
		} // end "adding cte" check
	}

	@Override
	public void visit(SeqScan seqscan) throws Exception {
		visit((Scan) seqscan);
	}

	@Override
	public void visit(Aggregate aggregate) throws Exception {
		saveRoot(aggregate);
		
		Select originalDST = null;
		if (dstStatement != null) originalDST = (Select)CCJSqlParserUtil.parse(dstStatement.toString());
		
		if (isRoot || aggregate.getAggregateID() == null) 
			aggregate.getChildren().get(0).accept(this);//.generateSQLStringDestOnly(dstStatement, false, stopAtJoin, allowedScans);
		else {
			dstStatement = null;
			aggregate.getChildren().get(0).accept(this);
		}
				
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();

		ps.getSelectItems().clear();
		
		for (String alias: aggregate.getOutSchema().keySet()) {
			
			Expression e = CCJSqlParserUtil.parseExpression(rewriteComplextOutItem(aggregate, aggregate.getOutSchema().get(alias).getSQLExpression()));
			SelectItem s = new SelectExpressionItem(e);
			
			if (!(e instanceof Column)) {
				((SelectExpressionItem)s).setAlias(new Alias(alias));
			}
			
			ps.addSelectItems(s);
		}
		
		// check if pruneToken or join token needs to be implemented
		
		List<Expression> updatedGroupBy = aggregate.updateGroupByElements(stopAtJoin);
		ps.setGroupByColumnReferences(updatedGroupBy);
		
		if (aggregate.getAggregateFilter() != null) {
			Expression e = CCJSqlParserUtil.parseCondExpression(aggregate.getAggregateFilter());
			ps.setHaving(e);
		}
		
		
		if (isRoot || aggregate.getAggregateID() == null) return;
		if (originalDST == null) {
			
			SubSelect ss = makeNewSubSelectUpdateDST(aggregate, dstStatement);
			originalDST = SelectUtils.buildSelectFromTable(new Table()); // immediately replaced
			((PlainSelect)originalDST.getSelectBody()).setFromItem(ss);
			
			dstStatement = originalDST;
			return;
		}
		
		SubSelect ss = makeNewSubSelectUpdateDST(aggregate, dstStatement);
		net.sf.jsqlparser.statement.select.Join insert = new net.sf.jsqlparser.statement.select.Join();
		insert.setRightItem(ss);
		insert.setSimple(true);
		
		PlainSelect pselect = (PlainSelect)originalDST.getSelectBody();
		
		if (pselect.getJoins() != null) {
			boolean isFound = false;
			
			Map<String, String> aliasMapping = aggregate.getDataObjectAliasesOrNames();
			
			for (int pos = 0; pos < pselect.getJoins().size(); pos++) { 
				FromItem r = pselect.getJoins().get(pos).getRightItem();
				if (!(r instanceof Table)) continue;
				
				Table t = (Table)r;
				if (t.getName().equals(aggregate.getAggregateToken())) {
					pselect.getJoins().remove(pos);
					isFound = true;
				} else if (t.getAlias() != null && aliasMapping.containsKey(t.getAlias().getName()) && 
						aliasMapping.get(t.getAlias().getName()).equals(t.getName())) {
					pselect.getJoins().remove(pos);
					isFound = true;
				}
			}
			if (!isFound) {
				for (int pos = 0; pos < pselect.getJoins().size(); pos++) { 
					if (pselect.getJoins().get(pos).isSimple()) {
						pselect.getJoins().add(pos, insert);
						break;
					}
				}
			} else {
				pselect.getJoins().add(insert);
			}
		}
		dstStatement = originalDST;
	}

	@Override
	public void visit(WindowAggregate windagg) throws Exception {
		saveRoot(windagg);
		windagg.getChildren().get(0).accept(this);
	}
	
	@Override
	public void visit(Limit limit) throws Exception {
		saveRoot(limit);
		limit.getChildren().get(0).accept(this);
		
		PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();

		net.sf.jsqlparser.statement.select.Limit sqllim = new net.sf.jsqlparser.statement.select.Limit();
		
		if (limit.isLimitAll()) sqllim.setLimitAll(limit.isLimitAll());
		else if (limit.isLimitNull()) sqllim.setLimitNull(limit.isLimitNull());
		else sqllim.setRowCount(limit.getLimitCount());
		
		if (limit.isHasOffSet()) sqllim.setOffset(limit.getLimitOffset());
		
		ps.setLimit(sqllim);
	}
	
	@Override
	public Join generateStatementForPresentNonJoinSegment(Operator operator, StringBuilder sb, boolean isSelect) throws Exception {
		
		// find the join		
		Operator child = operator;
		while (!(child instanceof Join) && !child.getChildren().get(0).isPruned()) 
			// then there could be one child only
			child = child.getChildren().get(0);
		
		Select outputSelect;
		Select originalDst = dstStatement;
		dstStatement = null;
		
		if ( !(operator instanceof Join) && (child instanceof Join)) {
			// TODO targeted strike? CURRENTLY WASH EVERYTHING // Set<String> names = child.getDataObjectNames();
			configure(true, true);
			setAllowedScan(operator.getDataObjectAliasesOrNames().keySet());
			operator.accept(this);
			outputSelect = dstStatement;
			
			Map<String, String> ane				= operator.getChildren().get(0).getDataObjectAliasesOrNames();
			Set<String> childAliases			= ane.keySet();
			Set<String> childAliasesAndNames	= new HashSet<>(ane.values());
			for (String s : ane.values()) childAliasesAndNames.add(s);
			
			populateComplexOutItem(operator, false);
			
			PlainSelect ps = ((PlainSelect)outputSelect.getSelectBody());
			List<SelectItem> sil = ps.getSelectItems();
			for (int i = 0 ; i < sil.size(); i ++) {
				if (sil.get(i) instanceof SelectExpressionItem) {
					SelectExpressionItem sei = (SelectExpressionItem)sil.get(i);
					sei.setExpression(CCJSqlParserUtil.parseExpression(rewriteComplextOutItem(operator, sei.getExpression())));
				}
			}
			
//			System.out.printf("\n\n\n---> childAlias&Names: %s\n\n\n", childAliasesAndNames);
			String token;
			if (child.isPruned()) token = child.getPruneToken();
    		else token = ((Join)child).getJoinToken();
				
			updateSubTreeTokens(ps, childAliases, childAliasesAndNames, token);
			if (outputSelect.getWithItemsList() != null) 
				for (WithItem wi : outputSelect.getWithItemsList())
					updateSubTreeTokens(((PlainSelect)wi.getSelectBody()), childAliases, childAliasesAndNames, token);
			
			operator.setSubTree(true);
			if (!isSelect) addSelectIntoToken(outputSelect, operator.getSubTreeToken());
			
			sb.append(outputSelect);
//		} else if (!(operator instanceof Join) && !(child instanceof Join)) {
//			operator.accept(this);
//			outputSelect = dstStatement; //operator.generateSQLStringDestOnly(null, true, true, operator.getDataObjectAliasesOrNames().keySet());
//			
//			operator.setSubTree(true);
//			if (!isSelect) addSelectIntoToken(outputSelect, operator.getSubTreeToken());
//			
//			throw new Exception ("---->> shouldn't be here: "+outputSelect);
//			
//			sb.append(outputSelect);
		} 
		
		dstStatement = originalDst;
		
		if (child instanceof Join)
			return (Join) child;
		else 
			return null;
	}
	
	/**
	 * OPERATOR
	 * @param into
	 * @param stopAtJoin
	 * @return
	 * @throws Exception
	 */
	@Override
	public String generateSelectIntoStatementForExecutionTree(String into) throws Exception {
		if (root == null) throw new Exception("SQLQueryGenerator, selectinto: root is null");
		postprocessSQLStatement(root);
		String output = addSelectIntoToken(dstStatement, into);
		return output;
	}
	
	/**
	 * OPERATOR
	 * @param operator
	 * @throws Exception
	 */
	private void postprocessSQLStatement(Operator operator) throws Exception {

//		boolean originalPruneStatus = operator.isPruned();
//		operator.prune(false);
//		
//		allowedScans = operator.getDataObjectAliasesOrNames().keySet();
//		operator.accept(this);
//		Select dstStatement  = this.generateSQLStringDestOnly(null, true, stopAtJoin, operator.getDataObjectAliasesOrNames().keySet());
//		operator.prune(originalPruneStatus);
		
		// iterate over out schema and add it to select clause
		HashMap<String, SelectItem> selects = new HashMap<String, SelectItem>();

		operator.updateObjectAliases();
		
		changeAttributesForSelectItems(operator, srcStatement, (PlainSelect) dstStatement.getSelectBody(), selects, stopAtJoin);
		
		if (operator.isAnyProgenyPruned()) {
			PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
			
			List<Operator> walker = operator.getChildren();
			List<Operator> nextgen;
			
			while (!walker.isEmpty()) {
				nextgen = new ArrayList<>();
				for (Operator o : walker) {
					
					if (!o.isPruned() && !(stopAtJoin && o instanceof Join) && !(stopAtJoin && o instanceof Aggregate && ((Aggregate)o).getAggregateID() != null)) {
						nextgen.addAll(o.getChildren());
						continue;
					}
						
					
					Map<String, String> ane				= o.getDataObjectAliasesOrNames();
					Set<String> childAliases			= ane.keySet();
					Set<String> childAliasesAndNames	= new HashSet<>(ane.values());
					for (String s : ane.values()) childAliasesAndNames.add(s);
					
					populateComplexOutItem(operator, false);
					
					List<SelectItem> sil = ps.getSelectItems();
					for (int i = 0 ; i < sil.size(); i ++) {
						if (sil.get(i) instanceof SelectExpressionItem) {
							SelectExpressionItem sei = (SelectExpressionItem)sil.get(i);
							sei.setExpression(CCJSqlParserUtil.parseExpression(rewriteComplextOutItem(operator, sei.getExpression())));
						}
					}
					
					String token;
					
					if (o.isPruned())
						token = o.getPruneToken();
					else if (o instanceof Join)
						token = ((Join)o).getJoinToken();
					else if (o instanceof Aggregate && ((Aggregate)o).getAggregateID() != null)
						token = ((Aggregate)o).getAggregateToken();
					else 
						throw new Exception("Uncovered case: "+o.getClass().getSimpleName());
						
					updateSubTreeTokens(ps, childAliases, childAliasesAndNames, token);
					if (dstStatement.getWithItemsList() != null) 
						for (WithItem wi : dstStatement.getWithItemsList())
							updateSubTreeTokens(((PlainSelect)wi.getSelectBody()), childAliases, childAliasesAndNames, token);
					
				}
				walker = nextgen;
			}
		}
	}
	
	/**
	 * OPERATOR
	 * @param o
	 * @param srcStatement
	 * @param ps
	 * @param selects
	 * @param stopAtJoin
	 * @throws Exception
	 */
	private void changeAttributesForSelectItems(Operator o, Select srcStatement, PlainSelect ps, HashMap<String, SelectItem> selects, boolean stopAtJoin) throws Exception {
		List<SelectItem> selectItemList = new ArrayList<>(); 
		
		for(String s : o.getOutSchema().keySet()) {
			SQLAttribute attr = new SQLAttribute((SQLAttribute)o.getOutSchema().get(s));

			// find the table where it is pruned
			changeAttributeName(o, attr, stopAtJoin);
			
			SelectExpressionItem si = new SelectExpressionItem(attr.getSQLExpression());
			
			if(!(si.toString().equals(attr.getName())) && !(attr.getSQLExpression() instanceof Column)) {
				si.setAlias(new Alias(attr.getFullyQualifiedName()));
			}
			
			if (srcStatement == null)
				selectItemList.add(si);
			else 
				selects.put(s, si);
		}
		
		if (srcStatement == null)
			ps.setSelectItems(selectItemList);
		else 
			ps.setSelectItems(changeSelectItemsOrder(srcStatement, selects));
		
	}
	
	/**
	 * OPERATOR
	 * @param attr
	 * @return
	 * @throws Exception
	 */
	private boolean changeAttributeName(Operator operator, SQLAttribute attr, boolean stopAtJoin) throws Exception {
		// if no children, then do nothing 
		// for each children, 
		// check if the child is pruned, 
		//     if so check if it bears the name; 
		//         if so, change the attribute name 

		Expression e = attr.getSQLExpression();
		Set<Column> attribsExpr = new HashSet<>(SQLExpressionUtils.getAttributes(e));
		Set<String> attribs = new HashSet<>();
		
		for (Column c : attribsExpr) attribs.add(c.getFullyQualifiedName());
		
		boolean ret = false;
		
		if (operator.getChildren().size() > 0) {
			for (Operator o : operator.getChildren()) {
				if (o.isPruned()) {
					
					if (o.getOutSchema().containsKey(attr.getName())) {
						
						Set<String> replacementSet = new HashSet<String>(operator.getObjectAliases());
						replacementSet.add(attr.getName());
						SQLExpressionUtils.renameAttributes(e, replacementSet, null, o.getPruneToken());
						
						attr.setExpression(e.toString());
						
						return true;
					} else if (attribs.removeAll(o.getOutSchema().keySet())) {
						
						Set<String> replacementSet = new HashSet<String>(operator.getObjectAliases());
						replacementSet.add(attr.getName());
						SQLExpressionUtils.renameAttributes(e, replacementSet, null, o.getPruneToken());
						
						attr.setExpression(e.toString());
						ret = true;
					}
				} else if (stopAtJoin && o instanceof Join) {
						if (o.getOutSchema().containsKey(attr.getName()) || attribs.removeAll(o.getOutSchema().keySet())) {
						
						if (e instanceof Column) ((Column)e).setTable(new Table(((Join)o).getJoinToken()));
						else e = new Column(new Table(((Join)o).getJoinToken()), attr.getName());
						
						attr.setExpression(e.toString());
						ret = true;
						break;
					}
				} else if (o instanceof Aggregate && ((Aggregate)o).getAggregateID() != null) {
					
					
					// TODO REDO: change the entire expression to match the output of children aggregates 
					// TODO SOLVE DUPLICATION PROBLEM
					
					if (o.getOutSchema().containsKey(attr.getName()) || attribs.removeAll(o.getOutSchema().keySet())) {
						
						if (e instanceof Column) ((Column)e).setTable(new Table(((Aggregate)o).getAggregateToken()));
						else e = new Column(new Table(((Aggregate)o).getAggregateToken()), attr.getName());
						
						attr.setExpression(e.toString());
						ret = true;
						break;
					}
				} else {
					if ( changeAttributeName(o, attr, stopAtJoin) ) return true;
				}
			}
		}
		
		return ret;
	}
	
	/**
	 * OPERATOR
	 * @param dstStatement
	 * @param into
	 * @return
	 */
	private String addSelectIntoToken(Select dstStatement, String into) {
		// dealing with WITH statment
		if (into != null) {
			
			if (dstStatement.getWithItemsList() == null) {
				addInto(dstStatement.getSelectBody(), into);
				return dstStatement.toString();
			}
			
			// single out the with statement 
			for (WithItem wi : dstStatement.getWithItemsList()) {
				if (wi.getName().equals(into)) {
					addInto(wi.getSelectBody(), into);
					dstStatement.getWithItemsList().remove(wi); // remove this item so it no longer gets bundled
					return wi.getSelectBody().toString();
				}
			}
		}
		return dstStatement.toString();
	}
	
	/**
	 * OPERATOR
	 * @param body
	 * @param into
	 */
	private void addInto(SelectBody body, String into) {
		Table t = new Table();
		t.setName(into);
		ArrayList<Table> tl = new ArrayList<>();
		tl.add(t);
		
		PlainSelect ps = (PlainSelect) body; 
		ps.setIntoTables(tl);
		
		
		List<String> columnNames = new ArrayList<>();
		List<SelectItem> seli = new ArrayList<>();
		SelectItemVisitor siv = new SelectItemVisitor() {
			@Override public void visit(AllColumns allColumns) {}
			@Override public void visit(AllTableColumns allTableColumns) {} // this is bad, but we can't do too much about it TODO
			@Override
			public void visit(SelectExpressionItem selectExpressionItem) {
				try {
					List<String> columns = SQLExpressionUtils.getColumnNamesInAllForms(selectExpressionItem.getExpression());
					if (!columns.removeAll(columnNames)) {
						columnNames.addAll(columns); // TODO EXTREMELY BAD PRACTICE. BETTER SOLUTION NEEDED
						seli.add(selectExpressionItem);
					}
					
				} catch (JSQLParserException e) {e.printStackTrace();}
			}
		};
		for (SelectItem si : ps.getSelectItems()) si.accept(siv);
		ps.setSelectItems(seli);
	}
	
	/**
	 * OPERATOR
	 * @param srcStatement
	 * @param selects
	 * @return
	 * @throws Exception
	 */
	private List<SelectItem> changeSelectItemsOrder(Select srcStatement, HashMap<String, SelectItem> selects) throws Exception {
		List<SelectItem> orders = ((PlainSelect) srcStatement.getSelectBody()).getSelectItems();
		List<SelectItem> holder = new ArrayList<>();
		
		SelectItemVisitor siv = new SelectItemVisitor() {

			@Override
			public void visit(AllColumns allColumns) {
				holder.add(allColumns);
			}

			@Override
			public void visit(AllTableColumns allTableColumns) {
				holder.add(allTableColumns);
			}

			@Override
			public void visit(SelectExpressionItem selectExpressionItem) {

				// find the child where the pruned token or seqscan or CTE is located, make it the corresponding position
				
				Expression e = selectExpressionItem.getExpression();
				
				
				SQLExpressionHandler deparser = new SQLExpressionHandler() {
					@Override
					public void visit(Column tableColumn) {
						String out = tableColumn.getFullyQualifiedName();
						if (selects.get(out) != null)
							holder.add(selects.get(out));
						else if (selects.get(out = tableColumn.getTable().getName()+ "."+ tableColumn.getColumnName()) != null)
							holder.add(selects.get(out));
						else {
							out = tableColumn.getFullyQualifiedName();
							// well.
							for (String s : selects.keySet()) {
								if (s.endsWith(out)){
									holder.add(selects.get(s));
									break;
								}
							}
						}
						selects.remove(out);
					}
					
					@Override
					public void visit(Parenthesis parenthesis) {
						parenthesis.getExpression().accept(this);
					}
					
					@Override
					public void visit(Function function) {
						holder.add(selectExpressionItem);
					}
					
					@Override
					protected void visitBinaryExpression(BinaryExpression binaryExpression, String operator) {
						holder.add(selectExpressionItem);
					}
				};
				
				e.accept(deparser);
				
			}
			
		};
		
		
		for (SelectItem si : orders) {
			si.accept(siv);
		}
		return holder;
	}
	
	/**
	 * OPERATOR
	 * @param ps
	 * @param originalAliases
	 * @param aliasesAndNames
	 * @param subTreeToken
	 * @throws Exception
	 */
	protected void updateSubTreeTokens(PlainSelect ps, Set<String> originalAliases, Set<String> aliasesAndNames, String subTreeToken) throws Exception {
		List<OrderByElement> obes 	= ps.getOrderByElements();
		List<Expression> gbes 		= ps.getGroupByColumnReferences();
		List<SelectItem> sis 		= ps.getSelectItems();
		Expression where = ps.getWhere();
		Expression having = ps.getHaving();
		
		// CHANGE WHERE AND HAVING
		if (where != null) SQLExpressionUtils.renameAttributes(where, originalAliases, aliasesAndNames, subTreeToken);
		if (having != null) SQLExpressionUtils.renameAttributes(having, originalAliases, aliasesAndNames, subTreeToken);
		
		// CHANGE ORDER BY
		if (obes != null && !obes.isEmpty()) 
			for (OrderByElement obe : obes) 
				SQLExpressionUtils.renameAttributes(obe.getExpression(), originalAliases, aliasesAndNames, subTreeToken);
		
		// CHANGE GROUP BY and SELECT ITEM
		if (gbes != null && !gbes.isEmpty()) {
			for (Expression gbe : gbes) 
				SQLExpressionUtils.renameAttributes(gbe, originalAliases, aliasesAndNames, subTreeToken);
		}
		for (SelectItem si : sis) {
			SelectItemVisitor siv = new SelectItemVisitor() {
				@Override public void visit(AllColumns allColumns) {}
				@Override public void visit(AllTableColumns allTableColumns) {}
				@Override public void visit(SelectExpressionItem selectExpressionItem) {
					try {
						SQLExpressionUtils.renameAttributes(selectExpressionItem.getExpression(), originalAliases, aliasesAndNames, subTreeToken);
					} catch (JSQLParserException e) {e.printStackTrace();}}};
			si.accept(siv);
		}
		
		// CHANGE FROM AND JOINS
		
		FromItemVisitor fv = new FromItemVisitor() {
			@Override public void visit(Table tableName) {}
			@Override public void visit(ValuesList valuesList) {}
			@Override public void visit(SubJoin subjoin) {subjoin.getLeft().accept(this);}
			@Override public void visit(LateralSubSelect lateralSubSelect) {lateralSubSelect.getSubSelect().accept(this);}

			@Override
			public void visit(SubSelect subSelect) {
				try { 
					updateSubTreeTokens((PlainSelect)subSelect.getSelectBody(), originalAliases, aliasesAndNames, subTreeToken);
				} catch (Exception e) { e.printStackTrace(); }
			}
		};
		
		ps.getFromItem().accept(fv);
		if (ps.getJoins() != null)
			for (net.sf.jsqlparser.statement.select.Join j : ps.getJoins())
				j.getRightItem().accept(fv);
		
	}
	
	/**
	 * OPERATOR
	 * @param o
	 * @param token
	 * @return
	 * @throws Exception
	 */
	protected Select generateSelectWithToken(Operator o, String token) throws Exception {
    	Select dstStatement = SelectUtils.buildSelectFromTable(new Table(token));
		PlainSelect ps = (PlainSelect)dstStatement.getSelectBody();
		List<SelectItem> lsi = new ArrayList<>();
		for (String s : o.getOutSchema().keySet()) {
			SelectExpressionItem sei = new SelectExpressionItem();
			Expression e = CCJSqlParserUtil.parseExpression(o.getOutSchema().get(s).getExpressionString());
			SQLExpressionUtils.renameAttributes(e, null, null, token);
			sei.setExpression(e);
			lsi.add(sei);
		}
		ps.setSelectItems(lsi);
		return dstStatement;
    }
	
	/**
	 * OPERATOR
	 * @param o
	 * @param first
	 */
	protected void populateComplexOutItem(Operator o, boolean first) {
		// populate complexOutItemFromProgeny
		for (Operator child : o.getChildren()){
			if ((!first) && child.getComplexOutItemFromProgeny().isEmpty()) populateComplexOutItem(child, first);
			for (String s: child.getOutSchema().keySet()) {
				Expression e = child.getOutSchema().get(s).getSQLExpression();
				if (e == null) continue;
				while (e instanceof Parenthesis) e = ((Parenthesis)e).getExpression();
				if (e instanceof Column) continue;
				o.getComplexOutItemFromProgeny().put(s, e.toString().replaceAll("[.]", "\\[\\.\\]").replaceAll("[(]", "\\[\\(\\]").replaceAll("[)]", "\\[\\)\\]"));
			}
			if (first || (!child.isPruned()
					|| (child instanceof Join && ((Join)child).getJoinID() == null)
					|| (child instanceof Aggregate && ((Aggregate)child).getAggregateID() == null))) o.getComplexOutItemFromProgeny().putAll(child.getComplexOutItemFromProgeny());
		}
	}
	
	/**
	 * OPERATOR
	 * @param expr
	 * @return
	 * @throws Exception
	 */
	protected String rewriteComplextOutItem(Operator o, String expr) throws Exception {
		// simplify
		expr = CCJSqlParserUtil.parseExpression(expr).toString();
		for (String alias : o.getComplexOutItemFromProgeny().keySet()) {
			expr = expr.replaceAll("("+o.getComplexOutItemFromProgeny().get(alias)+")", alias);
		}
		return expr;
	}
	
	/**
	 * OPERATOR
	 * @param e
	 * @return
	 * @throws Exception
	 */
	protected String rewriteComplextOutItem(Operator o, Expression e) throws Exception {
		// simplify
		String expr = e.toString();
		for (String alias : o.getComplexOutItemFromProgeny().keySet()) {
			expr = expr.replaceAll(o.getComplexOutItemFromProgeny().get(alias), alias);
		}
		return expr;
	}
	
	/**
	 * JOIN
	 * @param joinPred
	 * @param child0
	 * @param child1
	 * @param t0
	 * @param t1
	 * @param update
	 * @return
	 * @throws Exception
	 */
	protected String updateOnExpression(String joinPred, Operator child0, Operator child1, Table t0, Table t1, boolean update) throws Exception {
    	
    	Expression expr = CCJSqlParserUtil.parseCondExpression(joinPred);
		List<String> itemsSet = SQLExpressionUtils.getColumnTableNamesInAllForms(expr);
		
		if (!replaceTableNameWithPruneName(child0, expr, t0, itemsSet))
			findAndGetTableName(child0, t0, itemsSet);
		if (!replaceTableNameWithPruneName(child1, expr, t1, itemsSet))
			findAndGetTableName(child1, t1, itemsSet);
		
		return expr.toString();
	}

	/**
	 * JOIN
	 * @param child
	 * @param e
	 * @param t
	 * @param itemsSet
	 * @return
	 * @throws Exception
	 */
	private boolean replaceTableNameWithPruneName(Operator child, Expression e, Table t, List<String> itemsSet) throws Exception {
		if (child.isPruned()) {
			// does child have any of those names? 
			Set<String> names = new HashSet<>(child.getDataObjectAliasesOrNames().keySet());
			if (child.getObjectAliases() == null) child.updateObjectAliases();
			names.addAll(child.getObjectAliases());
			names.retainAll(itemsSet);
			if (names.size() > 0) {
				SQLExpressionUtils.renameAttributes(e, names, null, child.getPruneToken());
				t.setName(child.getPruneToken());
				return true;
			} else 
				return false;
		} else if (child instanceof Aggregate && ((Aggregate)child).getAggregateID() != null) {
			// does child have any of those names? 
			Set<String> names = new HashSet<>(child.getDataObjectAliasesOrNames().keySet());
			
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
	
	/**
	 * JOIN
	 * @param child
	 * @param t
	 * @param itemsSet
	 * @return
	 * @throws Exception
	 */
	private boolean findAndGetTableName(Operator child, Table t, List<String> itemsSet) throws Exception {
	    	
		Set<String> names = new HashSet<>(child.getDataObjectAliasesOrNames().keySet());
		child.updateObjectAliases();
		names.addAll(child.getObjectAliases());
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
	
	/**
	 * JOIN
	 * @param o
	 * @param zeroFirst
	 * @param foundExpression
	 * @return
	 * @throws Exception
	 */
	private List<String> processLeftAndRightWithIndexCond(Operator o, boolean zeroFirst, List<String> foundExpression) throws Exception {
    	
    	Map<String, Expression> child0Cond;
    	Map<String, Expression> child1Cond;

    	if (zeroFirst) {
    		child0Cond = o.getChildren().get(0).getChildrenPredicates();
    		child1Cond = o.getChildren().get(1).getChildrenPredicates();
    	} else {
    		child0Cond = o.getChildren().get(1).getChildrenPredicates();
    		child1Cond = o.getChildren().get(0).getChildrenPredicates();
    	}
    	
		for (String s : child0Cond.keySet()) {
			if (child0Cond.get(s) == null ) continue;
			List<Column> ls = SQLExpressionUtils.getAttributes(child0Cond.get(s));
			for (Column c : ls) {
				String s2 = c.getTable().getName();
				
				if (child1Cond.containsKey(s2)) {
					
					List<String> ret = new ArrayList<>();
					ret.add(s);
					ret.add(s2);
					
					foundExpression.add(child0Cond.get(s).toString());
//					System.out.printf("--->> join, processLeftAndRightWithIndexCond, found expression: %s\n\n", foundExpression);
					return ret;
				}
			}
		}
		return null;
    }
	
	/**
	 * JOIN
	 * @param dstStatement
	 * @param t
	 */
	private void addJSQLParserJoin(Select dstStatement, Table t) {
		net.sf.jsqlparser.statement.select.Join newJ = new net.sf.jsqlparser.statement.select.Join();
    	newJ.setRightItem(t);
    	newJ.setSimple(true);
    	if (((PlainSelect) dstStatement.getSelectBody()).getJoins() == null)
    		((PlainSelect) dstStatement.getSelectBody()).setJoins(new ArrayList<>());
    	((PlainSelect) dstStatement.getSelectBody()).getJoins().add(newJ);
	}
	
	/**
	 * JOIN
	 * @param s
	 * @param jf
	 * @return
	 */
	private void addToJoinFilter(String s, StringBuilder jf) {
    	if (s != null) { 
    		if (jf.length() == 0) 
    			jf.append(s); 
    		else jf .append(" AND ").append( s);
		}
    }

	/**
	 * AGGREGATE
	 * @param dstStatement
	 * @return
	 */
	private SubSelect makeNewSubSelectUpdateDST(Aggregate agg, Select dstStatement) {
		SubSelect ss = new SubSelect();
		ss.setAlias(new Alias(agg.getAggregateToken()));
		ss.setSelectBody(dstStatement.getSelectBody());
		return ss;
	}

	
	
	// consider moving this to a separator visitor
	
	@Override
	public String generateCreateStatementLocally(Operator op, String name) throws Exception {
		StringBuilder sb = new StringBuilder();
		
		sb.append("CREATE TABLE ").append(name).append(' ').append('(');
		
		boolean started = false;
		
		for (DataObjectAttribute doa : op.getOutSchema().values()) {
			if (started == true) sb.append(',');
			else started = true;
			
			sb.append(generateSQLTypedString(doa));
		}
		
		sb.append(')');
		
		return sb.toString();
	} 
	
	public String generateSQLTypedString(DataObjectAttribute doa) {
		return doa.getName().replaceAll(".+\\.(?=[\\w]+$)", "") + " " + convertTypeStringToSQLTyped(doa);
	}
	
	public String convertTypeStringToSQLTyped(DataObjectAttribute doa) {
		
		if (doa.getTypeString() == null || doa.getTypeString().charAt(0) == '*' || (doa.getTypeString().charAt(0) >= '0' && doa.getTypeString().charAt(0) <= '9'))
			return "integer";
		
		String str = doa.getTypeString().concat("     ").substring(0,5).toLowerCase();
		
		switch (str) {
		case "int32":
		case "int64":
			return "integer";
		case "strin":
			return "varchar";
		case "float":
			return "double precision";
		case "bool ":
			return "boolean";
		default:
			System.out.printf("--> abnormal type String: |%s|, |%s|;\n", str, doa.getTypeString());
			return doa.getTypeString();
		}
		
	}
	
	private String updatePruneTokensForOnExpression(Join j, String joinPred) throws Exception {
	    	
    	if (!j.isAnyProgenyPruned()) return new String(joinPred);
    	
    	List<Operator> lo = new ArrayList<>();
    	List<Operator> walker = j.getChildren();
    	while (!walker.isEmpty()) {
    		List<Operator> nextgen = new ArrayList<>();
    		for (Operator o : walker) {
    			if (o.isPruned()) lo.add(o);
    			else nextgen.addAll(o.getChildren());
    		}
    		walker = nextgen;
    	}
    	
    	Expression expr = CCJSqlParserUtil.parseCondExpression(joinPred);
    	for (Operator o : lo) {
    		Map<String, String> s = o.getDataObjectAliasesOrNames();
    		SQLExpressionUtils.renameAttributes(expr, s.keySet(), null, o.getPruneToken());
    	}
    	
		return expr.toString();
	}
	
	/**
	 * This one only supports equal sign and Column expressions
	 * @return
	 * @throws Exception
	 */
	public List<String> getJoinPredicateObjectsForBinaryExecutionNode(Join join) throws Exception {
		
		List<String> ret = new ArrayList<String>();
		
		if (join.getOriginalJoinPredicate() == null || join.getOriginalJoinPredicate().length() == 0) {
			
			Expression extraction = null;
			Column leftColumn = null;
			Column rightColumn = null;
			
			
			List<String> ses = processLeftAndRightWithIndexCond(join, true, null);
			String s, s2; 
			if (ses != null) {
				s = ses.get(0);
				s2 = ses.get(1);
				extraction = join.getChildren().get(0).getChildrenPredicates().get(s);
			} else {
				ses = processLeftAndRightWithIndexCond(join, false, null);
				if (ses == null) return ret;
				s = ses.get(1);
				s2 = ses.get(0);
				extraction = join.getChildren().get(1).getChildrenPredicates().get(s2);
			}
			
			
			List<Expression> exprs = SQLExpressionUtils.getFlatExpressions(extraction);
			for (Expression ex : exprs) {
				
				if (ex instanceof OrExpression) throw new Exception("SQLQueryGenerator: OrExpression encountered");
				if (!(ex instanceof EqualsTo)) throw new Exception("SQLQueryGenerator: Non-EqualsTo expression encountered");
				
				List<Column> ls = SQLExpressionUtils.getAttributes(ex);
				for (Column c2 : ls) if (c2.getTable().getName().equals(s)) {leftColumn = c2; break;}
				for (Column c2 : ls) if (c2.getTable().getName().equals(s2)) {rightColumn = c2; break;}
				
				while (extraction instanceof Parenthesis) extraction = ((Parenthesis)extraction).getExpression();
				ret.add(SQLExpressionUtils.getBinaryExpressionOperatorToken(ex));
				ret.add(String.format("{%s, %s}", leftColumn.getTable().getFullyQualifiedName(),leftColumn.getColumnName()));
				ret.add(String.format("{%s, %s}", rightColumn.getTable().getFullyQualifiedName(),rightColumn.getColumnName()));
			}
			
        	return ret;
		}
			
		
		
		Set<String> leftChildObjects = join.getChildren().get(0).getDataObjectAliasesOrNames().keySet();

//		System.out.println("---> Left Child objects: "+leftChildObjects.toString());
//		System.out.println("---> Right Child objects: "+rightChildObjects.toString());
//		System.out.println("---> joinPredicate: "+joinPredicate);
		
		Expression e = CCJSqlParserUtil.parseCondExpression(join.getOriginalJoinPredicate());
		List<Expression> exprs = SQLExpressionUtils.getFlatExpressions(e);
		
		for (Expression ex : exprs) {
			
			while (ex instanceof Parenthesis)
				ex = ((Parenthesis)ex).getExpression();
			
			if (ex instanceof OrExpression) throw new Exception("SQLQueryGenerator: OrExpression encountered");
			if (!(ex instanceof EqualsTo)) throw new Exception("SQLQueryGenerator: Non-EqualsTo expression encountered");
			
			// TODO SUPPORT MORE THAN COLUMN?
			
			Column left = (Column)((EqualsTo)ex).getLeftExpression();
			Column right = (Column)((EqualsTo)ex).getRightExpression();
			
			ret.add(SQLExpressionUtils.getBinaryExpressionOperatorToken(ex));
			if (leftChildObjects.contains(left.getTable().getName()) || leftChildObjects.contains(left.getTable().getFullyQualifiedName())) {
				ret.add(String.format("{%s, %s}", left.getTable().getFullyQualifiedName(),left.getColumnName()));
				ret.add(String.format("{%s, %s}", right.getTable().getFullyQualifiedName(),right.getColumnName()));
			} else {
				ret.add(String.format("{%s, %s}", right.getTable().getFullyQualifiedName(),right.getColumnName()));
				ret.add(String.format("{%s, %s}", left.getTable().getFullyQualifiedName(),left.getColumnName()));
			}
		}
//		System.out.println("---> joinPredicate ret: "+ret.toString()+"\n\n\n");
		
		return ret;
	}
	
	
}

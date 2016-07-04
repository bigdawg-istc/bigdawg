package istc.bigdawg.islands.SciDB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import istc.bigdawg.islands.DataObjectAttribute;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.PostgreSQL.SQLExpressionHandler;
import istc.bigdawg.islands.PostgreSQL.utils.SQLExpressionUtils;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandAggregate;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandJoin;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandOperator;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandScan;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandSort;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandWindowAggregate;
import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.CommonTableExpressionScan;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Limit;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.islands.operators.Sort;
import istc.bigdawg.islands.operators.WindowAggregate;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class AFLQueryGenerator implements OperatorVisitor {

//	StringBuilder sb = new StringBuilder();
	
	Stack<SciDBFunction> lastFunction = new Stack<>();
	
	boolean stopAtJoin = false;
	boolean isRoot = true;
	Operator root = null;
	final static String cross_join_token = "cross_join"; 
	final static String scan_token = "scan"; 
	
	@Override
	public void configure(boolean isRoot, boolean stopAtJoin) {
		this.stopAtJoin = stopAtJoin;
		this.isRoot = isRoot;
	}
	
	@Override
	public void reset(boolean isRoot, boolean stopAtJoin) {
		lastFunction = new Stack<>();
		this.stopAtJoin = stopAtJoin;
		this.isRoot = isRoot;
		root = null;
	}

	public void saveRoot(Operator o) {
		if (!this.isRoot) return;
		this.root = o;
		this.isRoot = false;
		lastFunction.clear();
	}
	
	@Override
	public void visit(Operator operator) throws Exception {
		throw new Exception("Unsupported Operator AFL output: Operator");
	}

	@Override
	public void visit(Join joinOp) throws Exception {
		
		SciDBIslandJoin join = (SciDBIslandJoin) joinOp;
		
		boolean isRootOriginal = isRoot;
		saveRoot(join);
		
		List<SciDBExpression> expressions = new ArrayList<>();
		
		join.getChildren().get(0).accept(this);
		if (!lastFunction.isEmpty() && !join.getAliases().isEmpty()) lastFunction.peek().alias = join.getAliases().get(0);
		expressions.add(lastFunction.pop());
		
		join.getChildren().get(1).accept(this);
		if (!lastFunction.isEmpty() && !join.getAliases().isEmpty()) lastFunction.peek().alias = join.getAliases().get(1);
		expressions.add(lastFunction.pop());
		

		if (join.generateJoinPredicate() != null) {
			
			String[] split = join.generateJoinPredicate().replaceAll("( AND )|( = )", ", ").replaceAll("[<>= ()]+", " ").replace("\\s+", ", ").split(", ");
			int pos = 0;
			for (String s : split ) {
				Column c = (Column) CCJSqlParserUtil.parseExpression(s);
				String tableName = c.getTable() == null ? null : c.getTable().toString();
				if (pos % 2 == 0 && join.getChildren().get(0).isPruned()) tableName = join.getChildren().get(0).getPruneToken();
				else if (pos % 2 == 1 && join.getChildren().get(0).isPruned()) tableName = join.getChildren().get(1).getPruneToken();
				expressions.add(new SciDBColumn(c.getColumnName(), null, tableName, isRootOriginal, null, null));
				pos ++;
			}
		}
		
		lastFunction.push(new SciDBFunction(cross_join_token, null, expressions, join.isPruned() ? join.getPruneToken() : null
				, join.getJoinToken(), isRootOriginal, new SciDBSchema(join.getOutSchema())));
		
	}

	@Override
	public void visit(Sort sortOp) throws Exception {
		
		SciDBIslandSort sort = (SciDBIslandSort) sortOp; 
		
		boolean isRootOriginal = isRoot;
		saveRoot(sort);
		
		List<SciDBExpression> expressions = new ArrayList<>();
		sort.getChildren().get(0).accept(this);
		expressions.add(lastFunction.pop());

		List<OrderByElement> obel = sort.updateOrderByElements();
		
		for (OrderByElement o : obel){
			Column c = ((Column)o.getExpression());
			expressions.add(new SciDBColumn(c.getColumnName(), null, c.getTable().toString(), false, null, null));
		}
		
		lastFunction.push(new SciDBFunction("sort", null, expressions, sort.isPruned() ? sort.getPruneToken() : null
				, sort.getSubTreeToken(), isRootOriginal, new SciDBSchema(sort.getOutSchema())));
	}

	@Override
	public void visit(Distinct distinct) throws Exception {
		throw new Exception("Unsupported Operator AFL output: Distinct");
	}

	@Override
	public void visit(Scan scanOp) throws Exception {
		
		SciDBIslandScan scan = (SciDBIslandScan) scanOp;
		
		boolean isRootOriginal = isRoot;
		
		saveRoot(scan);
		

		List<SciDBExpression> expressions = new ArrayList<>();
		if (scan.getChildren().isEmpty()) 
			expressions.add(new SciDBArrayHolder(scan.getSourceTableName(), null));
		else {
			scan.getChildren().get(0).accept(this);
			expressions.add(lastFunction.pop());
		}
		
		switch (scan.getOperatorName()) {
		case "apply":
			for (String s : scan.getOutSchema().keySet()){
				if (scan.getOutSchema().get(s).isHidden()) continue;
				if (scan.getOutSchema().get(s).getName().equals(scan.getOutSchema().get(s).getExpressionString())) continue;
				
				expressions.add(new SciDBColumn(s, null, null, false, null, null));
				DataObjectAttribute doa = scan.getOutSchema().get(s);
				expressions.add(convertSQLExpressionIntoSciDBApplyExpression(doa.getSQLExpression(), doa.getTypeString()));
			}
			
			break;
		case "project":
			for (String s : scan.getOutSchema().keySet()){
				if (scan.getOutSchema().get(s).isHidden()) continue;
				DataObjectAttribute doa = scan.getOutSchema().get(s);
				expressions.add(convertSQLExpressionIntoSciDBApplyExpression(doa.getSQLExpression(), doa.getTypeString()));
			}
			break;
		case "redimension":
			expressions.add(new SciDBSchema(scan.getOutSchema()));
			break;
		case "scan":
			break;
		case "filter":
			// BAD PRACTICE, BUT IT DOESN'T SEEM TO MATTER
			expressions.add(convertSQLExpressionIntoSciDBApplyExpression(scan.getFilterExpression(), null));
			break;
		default:
			break;
		}
		
		lastFunction.push(new SciDBFunction(scan.getOperatorName(), scan.isPruned() ? null : scan.getTableAlias(), expressions, scan.isPruned() ? scan.getPruneToken() : null
				, scan.getSubTreeToken(), isRootOriginal, new SciDBSchema(scan.getOutSchema())));
		

	}

	@Override
	public void visit(CommonTableExpressionScan cte) throws Exception {
		throw new Exception("Unsupported Operator AFL output: CTE");
	}

	@Override
	public void visit(SeqScan operator) throws Exception {
		visit((Scan)operator);
	}

	@Override
	public void visit(Aggregate aggregateOp) throws Exception {
		
		SciDBIslandAggregate aggregate = (SciDBIslandAggregate) aggregateOp;
		
		boolean isRootOriginal = isRoot;
		saveRoot(aggregate);
		
		List<SciDBExpression> expressions = new ArrayList<>();
		aggregate.getChildren().get(0).accept(this);
		expressions.add(lastFunction.pop());

		for (String s : aggregate.getOutSchema().keySet()) {
			DataObjectAttribute doa = aggregate.getOutSchema().get(s);
			if (!doa.getName().equalsIgnoreCase(doa.getExpressionString()) && doa.getExpressionString().contains("(")) {
				
				// now we just assume that it's in the format of 'sum(a)'
				String[] split = doa.getExpressionString().split("[\\(\\)\\s]+");
				
				List<SciDBExpression> templ = new ArrayList<>();
				templ.add(new SciDBColumn(split[1], null, null, false, null, null));
				
				expressions.add(new SciDBAggregationFunction(split[0], split.length < 3 ? null : split[2], templ));
			}
		}
		
		List<Expression> el = aggregate.updateGroupByElements(stopAtJoin);
		for (Expression e : el) {
			Column c = (Column) e;
			expressions.add(new SciDBColumn(c.getColumnName(), null, null, true, null, null));
		}
		
		lastFunction.push(new SciDBFunction("aggregate", null, expressions, aggregate.isPruned() ? aggregate.getPruneToken() : null
				, aggregate.getAggregateID() == null ? null : aggregate.getAggregateToken(), isRootOriginal, new SciDBSchema(aggregate.getOutSchema())));
		
	}

	@Override
	public void visit(WindowAggregate operator) throws Exception {

		SciDBIslandWindowAggregate window = (SciDBIslandWindowAggregate) operator;
		
		boolean isRootOriginal = isRoot;
		saveRoot(window);
		
		List<SciDBExpression> expressions = new ArrayList<>();
		window.getChildren().get(0).accept(this);
		expressions.add(lastFunction.pop());
		
		for (Integer i : window.getDimensionBounds())
			expressions.add(new SciDBConstant(i));
		
		for (String s : window.getOutSchema().keySet()) {
			DataObjectAttribute doa = window.getOutSchema().get(s);
			if (!doa.getName().equalsIgnoreCase(doa.getExpressionString()) && doa.getExpressionString().contains("(")) {
				
				System.out.printf("\ndoa expression string: %s; alias: %s\n\n", doa.getExpressionString(), doa.getName());
				
				// now we just assume that it's in the format of 'sum(a)'
				String[] split = doa.getExpressionString().split("[\\(\\)\\s]+");
				
				System.out.printf("the split: %s\n", Arrays.asList(split));
				
				List<SciDBExpression> templ = new ArrayList<>();
				templ.add(new SciDBColumn(split[1], null, null, false, null, null));
				
//				expressions.add(new SciDBAggregationFunction(split[0], split.length < 3 ? null : split[2], templ));
				expressions.add(new SciDBAggregationFunction(split[0], doa.getName(), templ));
			}
		}
		
		lastFunction.push(new SciDBFunction("window", null, expressions, window.isPruned() ? window.getPruneToken() : null
				, window.getSubTreeToken() == null ? null : window.getSubTreeToken(), isRootOriginal, new SciDBSchema(window.getOutSchema())));
	}

	@Override
	public void visit(Limit limit) throws Exception {
		throw new Exception("Unsupported Operator AFL output: Limit");
	}
	
	@Override
	public void visit(Merge merge) throws Exception {
		throw new Exception("Unsupported Operator AFL output: Merge");
	}

	@Override
	public String generateStatementString() throws Exception {
		if (!lastFunction.isEmpty()) return lastFunction.peek().toString();
		else return null;
	}

	@Override
	public Operator generateStatementForPresentNonMigratingSegment(Operator operator, StringBuilder sb, boolean isSelect)
			throws Exception {
		
//		if (true) throw new Exception("AFL generator Not updated for Merge");
		
		// find the join		
		Operator child = operator;
		while (!(child instanceof Join) && !child.getChildren().get(0).isPruned()) 
			child = child.getChildren().get(0);
		
		if ( !(operator instanceof Join) && (child instanceof Join)) {
			configure(true, true);
			operator.accept(this);
			String token;
			if (child.isPruned()) token = child.getPruneToken();
    		else token = ((Join)child).getJoinToken();
			lastFunction.push((SciDBFunction)lastFunction.pop().whiteWashArrayName(token));
			
			operator.setSubTree(true);
			if (!isSelect) {
				sb.append(generateSelectIntoStatementForExecutionTree(operator.getSubTreeToken()));
			} else {
				sb.append(lastFunction.peek());
			}
		} 
		
		if (child instanceof Join)
			return (Join) child;
		else 
			return null;
		
	}

	@Override
	public String generateSelectIntoStatementForExecutionTree(String destinationTable) throws Exception {
		StringBuilder sb2 = new StringBuilder();
		if (destinationTable != null && !lastFunction.isEmpty()) {
			sb2.append("store(").append(lastFunction.peek().toString()).append(", ").append(destinationTable).append(")");
		} else return null;
		return sb2.toString();
	}

	
	// consider separating the follows into a different file
	
	@Override
	public String generateCreateStatementLocally(Operator op, String name) throws Exception {
		StringBuilder sb = new StringBuilder();
		
		List<DataObjectAttribute> attribs = new ArrayList<>();
		List<DataObjectAttribute> dims = new ArrayList<>();
		
		for (DataObjectAttribute doa : ((SciDBIslandOperator) op).getOutSchema().values()) {
			if (doa.isHidden()) dims.add(doa);
			else attribs.add(doa);
		}
		
		sb.append("CREATE ARRAY ").append(name).append(' ').append('<');
		
		boolean started = false;
		for (DataObjectAttribute doa : attribs) {
			if (started == true) sb.append(',');
			else started = true;
			
			sb.append(generateAFLTypeString(doa));
		}
		
		sb.append('>').append('[');
		if (dims.isEmpty()) {
			sb.append("i=0:*,1000000,0");
		} else {
			started = false;
			for (DataObjectAttribute doa : dims) {
				if (started == true) sb.append(',');
				else started = true;
				
				sb.append(generateAFLTypeString(doa));
			}
		}
		sb.append(']');
		
		return sb.toString();
	}
	
	public String generateAFLTypeString(DataObjectAttribute doa) {
		
		char token = ':';
		if (doa.isHidden())
			token = '=';
		
		return doa.getName().replaceAll(".+\\.(?=[\\w]+$)", "") + token + convertTypeStringToAFLTyped(doa);
		
	}
	
	public String convertTypeStringToAFLTyped(DataObjectAttribute doa) {
		
		if (doa.getTypeString() == null) {
			System.out.println("Missing typeString: "+ doa.getName());
			return "int64";
		}
		
		if (doa.getTypeString().charAt(0) == '*' || (doa.getTypeString().charAt(0) >= '0' && doa.getTypeString().charAt(0) <= '9'))
			return doa.getTypeString();
		
		String str = doa.getTypeString().concat("     ").substring(0,5).toLowerCase();
		
		switch (str) {
		
		case "varch":
			return "string";
		case "times":
			return "datetime";
		case "doubl":
			return "double";
		case "integ":
		case "bigin":
			return "int64";
		case "boole":
			return "bool";
		default:
			return doa.getTypeString();
		}
	}

	
	/// the following should probably also go to a separate file
	
	
	public class SciDBExpression {
		String name = null;
		String alias = null;
		
		protected SciDBExpression(String name, String alias) {
			this.name = name;
			this.alias = alias;
		}
		
		public SciDBExpression whiteWashArrayName(String arrayToken) {
			return this;
		}
		
	}
	
	public class SciDBArrayHolder extends SciDBExpression {
		protected SciDBArrayHolder(String name, String alias) {
			super(name, alias);
		}
		
		@Override
		public SciDBExpression whiteWashArrayName(String arrayToken) {
			return new SciDBArrayHolder(arrayToken, alias);
		}
		
		@Override
		public String toString(){
			return name;
		}
	}
	
	public class SciDBUnaryExpression extends SciDBExpression {
		boolean isSigned = false;
		char sign = '-';
		
		public void setSign(char sign) {
			this.sign = sign;
		}
		
		protected SciDBUnaryExpression(String name, String alias) {
			super(name, alias);
		}
	}
	
	public class SciDBBinaryExpression extends SciDBExpression {
		
		SciDBExpression leftExpression = null;
		SciDBExpression rightExpression = null;
		boolean wrapLeft = false;
		boolean wrapRight = false;
		
		protected SciDBBinaryExpression(String name, SciDBExpression leftExpression, SciDBExpression rightExpression) {
			super(name, null);
			this.leftExpression = leftExpression;
			this.rightExpression = rightExpression;
			
			if (leftExpression == null || rightExpression == null) return;
			if (leftExpression.name.equalsIgnoreCase("or") && rightExpression.name.equalsIgnoreCase("and"))
				wrapLeft = true;
			if (leftExpression.name.equalsIgnoreCase("and") && rightExpression.name.equalsIgnoreCase("or"))
				wrapRight = true;
			
		}
		
		@Override
		public SciDBExpression whiteWashArrayName(String arrayToken) {
			return new SciDBBinaryExpression(name, leftExpression.whiteWashArrayName(arrayToken), rightExpression.whiteWashArrayName(arrayToken));
		}
		
		public SciDBExpression whiteWashArrayNameBySide(String leftReplacement, String rightReplacement) {
			return new SciDBBinaryExpression(name, leftExpression.whiteWashArrayName(leftReplacement), rightExpression.whiteWashArrayName(rightReplacement));
		}
		
		@Override
		public String toString() {
			if (leftExpression == null || rightExpression == null) return null;
			StringBuilder sb = new StringBuilder();
			
			if (wrapLeft) sb.append('(');
			sb.append(leftExpression.toString());
			if (wrapLeft) sb.append(") ");
			
			sb.append(name);
			
			if (wrapRight) sb.append(" (");
			sb.append(rightExpression.toString());
			if (wrapRight) sb.append(')'); 
			
			return sb.toString();
		}
		
	}
	
	public class SciDBFunction extends SciDBUnaryExpression {
		List<SciDBExpression> expressions = null;
		String pruneToken = null;
		String subTreeToken = null;
		boolean isFunctionRoot = false;
		SciDBSchema schema = null;
		
		//TODO 
		//TODO Properly update tokens.
		//TODO 
		
		public SciDBFunction(String name, String alias, List<SciDBExpression> expressions, String pruneToken, String subTreeToken, boolean isFunctionRoot, SciDBSchema schema) {
			super(name, alias);
			this.expressions = new ArrayList<>(expressions);
			this.pruneToken = pruneToken;
			this.subTreeToken = subTreeToken;
			this.isFunctionRoot = isFunctionRoot;
			this.schema = schema;
		}
		
		@Override
		public SciDBExpression whiteWashArrayName(String arrayToken) {
			List<SciDBExpression> exprs = new ArrayList<>();
			for (SciDBExpression e : expressions) {
				exprs.add(e.whiteWashArrayName(arrayToken));
			}
			return new SciDBFunction(name, alias, exprs, pruneToken, subTreeToken, isFunctionRoot, schema);
		}
		
		@Override
		public String toString() {
			if (pruneToken != null && !isFunctionRoot) return pruneToken; 
			if (name.equals(cross_join_token) && !isFunctionRoot && stopAtJoin) return subTreeToken;
			
			if (!isFunctionRoot && name.equalsIgnoreCase(scan_token)) return expressions.get(0).name;
			StringBuilder strb = new StringBuilder();
			
			// name
			strb.append(name).append('(');
			
			// each of the expressions
			boolean started = false;
			for (SciDBExpression e : expressions) {
				if (started) strb.append(", ");
				else started = true;
				strb.append(e.toString());
				if (e.alias != null && !e.alias.equals(expressions.get(0).toString())) {
					strb.append(" AS ").append(e.alias);
				}
			}
			strb.append(')');
			
			// end
			return strb.toString();
		}
	}
	
	public class SciDBColumn extends SciDBUnaryExpression {
		String arrayName = null;
		boolean isDimension = false;
		List<String> dimensionParameters = null;
		String typeString = null;
		
		public SciDBColumn(String name, String alias, String arrayName, boolean isDimension, List<String> dimensionParameters, String typeString) {
			super(name, alias);
			this.arrayName = arrayName;
			this.isDimension = isDimension;
			if (dimensionParameters != null) this.dimensionParameters = new ArrayList<>(dimensionParameters);
			this.typeString = typeString;
		}
		
		public String getAttributeAndType() {
			if (isDimension || typeString == null) return null;
			StringBuilder strb = new StringBuilder();
			strb.append(name).append(':').append(typeString);
			return strb.toString(); 
		}
		
		public String getDimensionAndParameters() {
			if (!isDimension || dimensionParameters.size() < 4) return null;
			StringBuilder strb = new StringBuilder();
			strb.append(name).append('=')
				.append(dimensionParameters.get(0)).append(':')
				.append(dimensionParameters.get(1)).append(',')
				.append(dimensionParameters.get(2)).append(',')
				.append(dimensionParameters.get(3));
			return strb.toString(); 
		}
		
		@Override
		public SciDBExpression whiteWashArrayName(String arrayToken) {
			return new SciDBColumn(name, alias, arrayToken, isDimension, dimensionParameters, typeString);
		}
		
		@Override
		public String toString() {
			StringBuilder strb = new StringBuilder();
			if (arrayName != null) strb.append(arrayName).append('.');
			strb.append(name);
			return isSigned ? sign + ' ' + strb.toString() : strb.toString(); 
		}
	}
	
	public class SciDBConstant extends SciDBUnaryExpression {
		Object content = null;
		
		public SciDBConstant (Object content) {
			super(null, null);
			this.content = content;
		}
		
		@Override
		public String toString() {
			if (content == null) return null;
			else return isSigned ? sign + ' ' + content.toString() : content.toString();
		}
	}
	
	public class SciDBAggregationFunction extends SciDBExpression {
		
		List<SciDBExpression> content = null;
			
		protected SciDBAggregationFunction(String name, String alias, List<SciDBExpression> content) {
			super(name, alias);
			if (content != null && !content.isEmpty()) this.content = new ArrayList<>(content);
		}
		
		@Override
		public SciDBExpression whiteWashArrayName(String arrayToken) {
			List<SciDBExpression> exprs = new ArrayList<>();
			for (SciDBExpression e : content) {
				exprs.add(e.whiteWashArrayName(arrayToken));
			}
			return new SciDBAggregationFunction(name, alias, exprs);
		}
		
		@Override 
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append(name).append('(');
			
			boolean started = false;
			for (SciDBExpression e : content) {
				if (started) sb.append(", ");
				else started = true;
				sb.append(e.toString());
			} 
			sb.append(')');
			
//			if (alias != null) sb.append(" AS ").append(alias);
			return sb.toString();
		}
		
	}
	
	public class SciDBSchema extends SciDBExpression {
		
		List<SciDBColumn> attributes = new ArrayList<>();
		List<SciDBColumn> dimensions = new ArrayList<>();
		
		protected SciDBSchema(Map<String, DataObjectAttribute> outSchema) {
			super (null, null);
			
			for (String s : outSchema.keySet()) {
				if (outSchema.get(s).isHidden()) {
					dimensions.add(new SciDBColumn(outSchema.get(s).getName(), null, null, true, Arrays.asList(outSchema.get(s).getTypeString().split("[:,]")), null));
				} else {
					attributes.add(new SciDBColumn(outSchema.get(s).getName(), null, null, false, null, outSchema.get(s).getTypeString()));
				}
			}
		};
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			sb.append('<');
			boolean started = false;
			for (SciDBColumn c : attributes) {
				if (started) sb.append(',');
				else started = true;
				sb.append(c.getAttributeAndType());
			}
			sb.append('>').append('[');
			started = false;
			for (SciDBColumn c : dimensions) {
				if (started) sb.append(',');
				else started = true;
				sb.append(c.getDimensionAndParameters());
			}
			sb.append(']');
			return sb.toString();
		}
	}
	
	
	private SciDBExpression convertSQLExpressionIntoSciDBApplyExpression(Expression expr, String doaTypeString) {
		
		Stack<SciDBExpression> lastExpression = new Stack<>();
		
		SQLExpressionHandler deparser = new SQLExpressionHandler() {
			@Override
		    public void visit(Parenthesis parenthesis) {
				parenthesis.getExpression().accept(this);
		    }
			
			@Override
		    public void visit(InExpression ine) {
				System.out.printf("------> SHOULDN'T BE HERE: AFL PARSER, CONVERT, InExpression");
			}
			
			@Override
			public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expression, String operator) {
				
				SciDBExpression left = null;
				SciDBExpression right = null;
				
				Expression e = expression.getLeftExpression();
				e.accept(this);
				left = lastExpression.pop();
				
				e = expression.getRightExpression();
				e.accept(this);
				right = lastExpression.pop();
				
				lastExpression.push(new SciDBBinaryExpression(operator, left, right));
				
			}
			
			@Override
			protected void visitBinaryExpression(BinaryExpression expression, String operator) {
				
				SciDBExpression left = null;
				SciDBExpression right = null;
				
				Expression e = expression.getLeftExpression();
				e.accept(this);
				right = lastExpression.pop();
				
				e = expression.getRightExpression();
				e.accept(this);
				left = lastExpression.pop();
				
				lastExpression.push(new SciDBBinaryExpression(operator, left, right));
			}
			
			@Override
		    public void visit(ExpressionList expressionList) {
				System.out.printf("------> SHOULDN'T BE HERE: AFL PARSER, CONVERT, ExpressionList");
		    }
			
			@Override
			public void visit(MultiExpressionList multiExprList) {
				System.out.printf("------> SHOULDN'T BE HERE: AFL PARSER, CONVERT, MultiExpressionList");
			}
			
			@Override
			public void visit(Function function) {
				
				List<Expression> el = function.getParameters().getExpressions();
				List<SciDBExpression> sel = new ArrayList<>();
				
				for (Expression e : el) {
					e.accept(this);
					sel.add(lastExpression.pop());
				} 
				
				lastExpression.push(new SciDBAggregationFunction(function.getName(), null, sel));
				
			}
			
			@Override
			public void visit(SignedExpression se) {
				se.getExpression().accept(this);
//				if (lastExpression.peek() instanceof SciDBUnaryExpression) 
				((SciDBUnaryExpression)lastExpression.peek()).setSign(se.getSign());
			}
			
			@Override
		    public void visit(CaseExpression caseExpression) {
				System.out.printf("------> SHOULDN'T BE HERE: AFL PARSER, CONVERT, Case");
		    }
			
			@Override 
			public void visit(IsNullExpression inv) {
				System.out.printf("------> SHOULDN'T BE HERE: AFL PARSER, CONVERT, IsNullExpression");
			};
			
			@Override 
			public void visit(AnalyticExpression ae) {
				System.out.printf("------> SHOULDN'T BE HERE: AFL PARSER, CONVERT, AnalyticExpression");
			}
			
			@Override public void visit(IntervalExpression ie)  {}
			@Override public void visit(Column tableColumn) {
				lastExpression.push(new SciDBColumn(tableColumn.getColumnName(), null
						, tableColumn.getTable().toString().length() == 0 ? null : tableColumn.getTable().toString() , false, null, doaTypeString)); // TODO THIS IS JANKY.
			}
			@Override public void visit(LongValue lv) {
				lastExpression.push(new SciDBConstant(lv.getValue()));
			};
			@Override public void visit(DoubleValue lv) {
				lastExpression.push(new SciDBConstant(lv.getValue()));
			};
			@Override public void visit(HexValue lv) {
				lastExpression.push(new SciDBConstant(lv.getValue()));
			};
			@Override public void visit(NullValue lv) {
				lastExpression.push(new SciDBConstant("null"));
			};
			@Override public void visit(TimeValue lv) {
				lastExpression.push(new SciDBConstant("datetime(\\'"+lv.getValue()+"\\')"));
			};
			@Override public void visit(StringValue sv) {
				lastExpression.push(new SciDBConstant("\'"+sv.getValue()+"\'"));
			};
		};
		expr.accept(deparser);
		
		
		return lastExpression.pop();
	}

	
	@Override
	public List<String> getJoinPredicateObjectsForBinaryExecutionNode(Join join) throws Exception {
//		throw new Exception("Unsupported function for AFL visitor: getJoinPredicateObjectsForBinaryExecutionNode");
		
		
		List<String> ret = new ArrayList<String>();
		
		
		Set<String> leftChildObjects = join.getChildren().get(0).getDataObjectAliasesOrNames().keySet();

		System.out.println("---> Left Child objects: "+leftChildObjects.toString());
		System.out.println("---> Right Child objects: "+join.getChildren().get(1).getDataObjectAliasesOrNames().keySet().toString());
		System.out.println("---> joinPredicate: "+join.generateJoinPredicate());
		
		String preds = join.generateJoinPredicate();
		if (preds == null) preds = join.generateJoinFilter();
		if (preds == null) return ret;
		
		Expression e = CCJSqlParserUtil.parseCondExpression(preds);
		List<Expression> exprs = SQLExpressionUtils.getFlatExpressions(e);
		
		for (Expression ex : exprs) {
			
			while (ex instanceof Parenthesis)
				ex = ((Parenthesis)ex).getExpression();
			
			if (ex instanceof OrExpression) throw new Exception("AFLQueryGenerator: OrExpression encountered");
			if (!(ex instanceof EqualsTo)) throw new Exception("AFLQueryGenerator: Non-EqualsTo expression encountered");
			
			// TODO SUPPORT MORE THAN COLUMN?
			
			Column left = (Column)((EqualsTo)ex).getLeftExpression();
			Column right = (Column)((EqualsTo)ex).getRightExpression();
			
			ret.add(SQLExpressionUtils.getBinaryExpressionOperatorToken(ex));
			if (leftChildObjects.contains(left.getTable().getName()) || leftChildObjects.contains(left.getTable().getFullyQualifiedName())) {
				ret.add(left.getTable().getFullyQualifiedName());
				ret.add(left.getColumnName());
				ret.add(right.getTable().getFullyQualifiedName());
				ret.add(right.getColumnName());
			} else {
				ret.add(right.getTable().getFullyQualifiedName());
				ret.add(right.getColumnName());
				ret.add(left.getTable().getFullyQualifiedName());
				ret.add(left.getColumnName());
			}
//			System.out.printf("---> SQLQueryGenerator joinPredicate ret: %s\n", ret.toString());
		}
		
		System.out.printf("\n---->>>> getJoinPredForBin ret last: %s\n", ret);
		
		return ret;
		
		
	}

	
}

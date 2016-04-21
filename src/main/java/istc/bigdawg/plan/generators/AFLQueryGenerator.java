package istc.bigdawg.plan.generators;

import java.util.ArrayList;
import java.util.List;

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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class AFLQueryGenerator implements OperatorVisitor {

	StringBuilder sb = new StringBuilder();
	boolean stopAtJoin = false;
	boolean isRoot = true;
	Operator root = null;
	
	@Override
	public void configure(boolean isRoot, boolean stopAtJoin) {
		this.stopAtJoin = stopAtJoin;
		this.isRoot = isRoot;
	}

	public void saveRoot(Operator o) {
		if (!this.isRoot) return;
		this.root = o;
		this.isRoot = false;
		sb = new StringBuilder();
	}
	
	@Override
	public void visit(Operator operator) throws Exception {
	}

	@Override
	public void visit(Join join) throws Exception {
		
		//TODO
		//TODO consider intra island case
		//TODO
		
		if (join.isPruned() && !isRoot) {
			sb.append(join.getPruneToken());
			return;
		}
		
		if (stopAtJoin && !isRoot) {
			sb.append(join.getJoinToken());
			return;
		}
		
		saveRoot(join);
		
		sb.append("cross_join(");
		
		if (join.getChildren().get(0).isPruned())
			sb.append(join.getChildren().get(0).getPruneToken());
		else 
			join.getChildren().get(0).accept(this);//generateAFLString(recursionLevel+1));
		
		if (!join.getAliases().isEmpty()) 
			sb.append(" as ").append(join.getAliases().get(0));
		sb.append(", ");
		
		if (join.getChildren().get(1).isPruned())
			sb.append(join.getChildren().get(1).getPruneToken());
		else 
			join.getChildren().get(1).accept(this);//generateAFLString(recursionLevel+1));
		
		if (!join.getAliases().isEmpty()) sb.append(" as ").append(join.getAliases().get(1));
		
		if (join.getOriginalJoinPredicate() != null) {
			sb.append(", ");
			sb.append(join.getOriginalJoinPredicate().replaceAll("( AND )|( = )", ", ").replaceAll("[<>= ()]+", " ").replace("\\s+", ", "));
		}
		
		sb.append(')');
//		return sb.toString();
	}

	@Override
	public void visit(Sort sort) throws Exception {
		
		saveRoot(sort);
		
		sb.append("sort(");
		sort.getChildren().get(0).accept(this);
		if (!sort.getSortKeys().isEmpty()) {

			sort.updateOrderByElements();
			
			for (OrderByElement obe: sort.getOrderByElements()) {
				sb.append(", ").append(obe.toString());
			}
			
		}
		sb.append(')');
	}

	@Override
	public void visit(Distinct distinct) throws Exception {
		throw new Exception("Unsupported Operator AFL output: Distinct");
	}

	@Override
	public void visit(Scan scan) throws Exception {
		
		boolean isRootOriginal = isRoot;
		
		saveRoot(scan);
		
		if (!(scan.getOperatorName().equals("scan") && !isRootOriginal))
			sb.append(scan.getOperatorName()).append('(');
		
		boolean ped = (!scan.getChildren().isEmpty()) && scan.getChildren().get(0).isPruned();
		
		if (ped)
			sb.append(scan.getChildren().get(0).getPruneToken());
		else if (scan.getChildren().size() > 0)
			scan.getChildren().get(0).accept(this);
		else {
			sb.append(scan.getSrcTable());
		} 
		
		switch (scan.getOperatorName()) {
		case "apply":
			for (String s : scan.getOutSchema().keySet()){
				if (scan.getOutSchema().get(s).isHidden()) continue;
				if (scan.getOutSchema().get(s).getName().equals(scan.getOutSchema().get(s).getExpressionString())) continue;
				sb.append(", ").append(s).append(", ");
//					if (ped) {
//						// the apply
//						Expression expression = CCJSqlParserUtil.parseExpression(outSchema.get(s).getExpressionString());
//						Set<String> nameSet = new HashSet<>( SQLExpressionUtils.getColumnTableNamesInAllForms(expression));
//						SQLExpressionUtils.renameAttributes(expression, nameSet, nameSet, getChildren().get(0).getPruneToken());
//						sb.append(expression.toString());
//					} else {
					sb.append(scan.getOutSchema().get(s).getExpressionString());
//					}
			}
			
			break;
		case "project":
			for (String s : scan.getOutSchema().keySet()){
				if (scan.getOutSchema().get(s).isHidden()) continue;
				
				sb.append(", ");
				if (ped) {
					String[] o = scan.getOutSchema().get(s).getName().split("\\.");
					sb.append(scan.getChildren().get(0).getPruneToken()).append('.').append(o[o.length-1]);
				} else 
					sb.append(scan.getOutSchema().get(s).getName());
			}
			break;
		case "redimension":
			sb.append(", <");
			
			for (String s : scan.getOutSchema().keySet()) {
				if (scan.getOutSchema().get(s).isHidden()) continue;
				if (sb.charAt(sb.length()-1) != '<') sb.append(',');
				sb.append(generateAFLTypeString(scan.getOutSchema().get(s)));
			}
			sb.append(">[");
			for (String s : scan.getOutSchema().keySet()) {
				if (!scan.getOutSchema().get(s).isHidden()) continue;
				if (sb.charAt(sb.length()-1) != '[') sb.append(',');
				sb.append(generateAFLTypeString(scan.getOutSchema().get(s)));
			}
			sb.append(']');
			break;
		case "scan":
			break;
		case "filter":
			sb.append(", ").append(scan.getFilterExpression());
			break;
		default:
			break;
		}
			
		if (!(scan.getOperatorName().equals("scan") && !isRootOriginal))
			sb.append(')');
		
	}

	@Override
	public void visit(CommonSQLTableExpressionScan cte) throws Exception {
		throw new Exception("Unsupported Operator AFL output: CTE");
	}

	@Override
	public void visit(SeqScan operator) throws Exception {
		visit((Scan)operator);
	}

	@Override
	public void visit(Aggregate aggregate) throws Exception {
		
		//TODO
		//TODO CHECK SQL CODE FOR AGGREGATION HANDLING
		//TODO
		saveRoot(aggregate);
		sb.append("Aggregate(");
		aggregate.getChildren().get(0).accept(this);//generateAFLString(recursionLevel+1));
		
		// TODO make sure the GroupBy are marked as hidden, otherwise do a redimension
		
		
		for (String s : aggregate.getOutSchema().keySet()) {
			if (aggregate.getOutSchema().get(s).isHidden()) continue;
			sb.append(", ").append(aggregate.getOutSchema().get(s).getExpressionString());
			if (!aggregate.getOutSchema().get(s).getName().contains("(")) sb.append(" AS ").append(aggregate.getOutSchema().get(s).getName());
		}
		
		List<Expression> updatedGroupBy = aggregate.updateGroupByElements(false);
		
		if(updatedGroupBy.size() > 0) {
			for(Expression e : updatedGroupBy) {
				sb.append(", ").append(e);
			}
		}

		sb.append(')');
		
	}

	@Override
	public void visit(WindowAggregate operator) throws Exception {
		throw new Exception("Unsupported Operator AFL output: WindowAggregate");
		
	}

	@Override
	public void visit(Limit limit) throws Exception {
		throw new Exception("Unsupported Operator AFL output: Limit");
	}

	@Override
	public String generateStatementString() throws Exception {
		return sb.toString();
	}

	@Override
	public Join generateStatementForPresentNonJoinSegment(Operator operator, StringBuilder sb, boolean isSelect)
			throws Exception {
		//TODO
		//TODO CONSIDER
		//TODO
		
		return null;
	}

	@Override
	public String generateSelectIntoStatementForExecutionTree(String destinationTable) throws Exception {
		StringBuilder sb2 = new StringBuilder();
		if (destinationTable != null) {
			sb2.append("store(").append(sb).append(", ").append(destinationTable).append(')');
		}
		return sb2.toString();
	}

	
	// consider separating the follows into a different file
	
	@Override
	public String generateCreateStatementLocally(Operator op, String name) throws Exception {
		StringBuilder sb = new StringBuilder();
		
		List<DataObjectAttribute> attribs = new ArrayList<>();
		List<DataObjectAttribute> dims = new ArrayList<>();
		
		for (DataObjectAttribute doa : op.getOutSchema().values()) {
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

}

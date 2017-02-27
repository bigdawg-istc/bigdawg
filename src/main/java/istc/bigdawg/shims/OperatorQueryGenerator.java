package istc.bigdawg.shims;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.exceptions.ShimException;
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

public interface OperatorQueryGenerator {
	public void configure(boolean isRoot, boolean stopAtJoin);
	public void reset(boolean isRoot, boolean stopAtJoin);
	public void visit(Operator operator) throws Exception;
	public void visit(Join join) throws Exception;
	public void visit(Sort sort) throws Exception;
	public void visit(Distinct distinct) throws Exception;
	public void visit(Scan scan) throws Exception;
	public void visit(CommonTableExpressionScan cte) throws Exception;
	public void visit(SeqScan operator) throws Exception;
	public void visit(Aggregate aggregate) throws Exception;
	public void visit(WindowAggregate operator) throws Exception;
	public void visit(Limit limit) throws Exception;
	public void visit(Merge merge) throws Exception;
	public String generateStatementString() throws Exception;
	/**
	 * This function is used for multi-engine intra-island queries; it gives two forms of outputs:
	 * - return null or the first Join/Merge/etc. migrating operator further down in the tree
	 * - if the operator input is a Join/Merge/etc., do not modify the StringBuilder; otherwise fill the StringBuilder with the string of query that treat the first join as a scan; 
	 * To illustrate the second output in case of a non-join operator input, 
	 * if the operator original gives "SELECT count(*) FROM a JOIN b WHERE b.value < 3", 
	 * this function will fill the StringBuilder with something like "SELECT count(*) FROM BIGDAWGSQLJOINTOKEN_1"
	 * @param operator
	 * @param sb
	 * @param isSelect
	 * @return null or the first Join Operator down the tree
	 * @throws Exception
	 */
	public Pair<Operator, String> generateStatementForPresentNonMigratingSegment(Operator operator, boolean isSelect) throws Exception;
	public String generateSelectIntoStatementForExecutionTree(String destinationTable) throws Exception;
	
	/**
	 * This function is used in executing multi-engine intra-island shuffle joins. Currently only used for relational island.
	 * @param join
	 * @return Required content and order: Comparator string ("="), left table name, left column name, right table name, right column name
	 * @throws Exception
	 */
	public List<String> getJoinPredicateObjectsForBinaryExecutionNode(Join join) throws Exception;
	public String generateCreateStatementLocally(Operator op, String name) throws Exception;
}

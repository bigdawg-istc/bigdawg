package istc.bigdawg.shims;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.exceptions.ShimException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.query.ConnectionInfo;


/**
 * 
 * @author k2i
 *
 * A bridge between an island and an engine
 */
public interface Shim {
	
	public void connect(ConnectionInfo ci) throws ShimException;
	public void disconnect() throws ShimException;
	public String getSelectQuery(Operator root) throws ShimException;
	public String getSelectIntoQuery(Operator root, String dest, boolean stopsAtJoin) throws ShimException;
	public Pair<Operator, String> getQueryForNonMigratingSegment(Operator operator, boolean isSelect) throws ShimException;
	public List<String> getJoinPredicate(Join join) throws ShimException;
	
	public Optional<QueryResult> executeForResult(String query) throws ShimException;
	public void executeNoResult(String query) throws ShimException;
	public void dropTableIfExists(String name) throws ShimException;
	
}

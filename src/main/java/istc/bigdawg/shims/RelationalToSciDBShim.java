package istc.bigdawg.shims;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.exceptions.ShimException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandJoin;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.operators.SQLIslandOperator;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

public class RelationalToSciDBShim implements Shim {

	private SciDBConnectionInfo ci = null;
	
	@Override
	public void connect(ConnectionInfo ci) throws ShimException {
		if (ci instanceof SciDBConnectionInfo) {
			this.ci = (SciDBConnectionInfo) ci;
		} else {
			throw new ShimException("Input Connection Info is not SciDBConnectionInfo; rather: "+ ci.getClass().getName());
		}
	}

	@Override
	public void disconnect() throws ShimException {
		//// intentionally left blank
	}

	@Override
	public String getSelectQuery(Operator root) throws ShimException {
		
		assert(root instanceof SQLIslandOperator);
		
		OperatorQueryGenerator gen = new RelationalAFLQueryGenerator();
		gen.configure(true, false);
		try {
			root.accept(gen);	
			return gen.generateStatementString();
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
		
	}

	@Override
	public String getSelectIntoQuery(Operator root, String dest, boolean stopsAtJoin) throws ShimException {
		
		assert(root instanceof SQLIslandOperator);
		
		OperatorQueryGenerator gen = new RelationalAFLQueryGenerator();
		gen.configure(true, stopsAtJoin);
		try {
			root.accept(gen);	
			return gen.generateSelectIntoStatementForExecutionTree(dest);
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
	}

	@Override
	public Pair<Operator, String> getQueryForNonMigratingSegment(Operator operator, boolean isSelect)
			throws ShimException {
		
		assert(operator instanceof SQLIslandOperator);
		
		OperatorQueryGenerator gen = new RelationalAFLQueryGenerator();
		gen.configure(true, false);
		try {
			operator.accept(gen);	
			return gen.generateStatementForPresentNonMigratingSegment(operator, isSelect);
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
	}
	
	@Override
	public List<String> getJoinPredicate(Join join) throws ShimException {
		
		assert(join instanceof SciDBIslandJoin);
		
		OperatorQueryGenerator gen = new RelationalAFLQueryGenerator();
		gen.configure(true, false);
		try {
			join.accept(gen);	
			return gen.getJoinPredicateObjectsForBinaryExecutionNode(join);
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
	};

	@Override
	public Optional<QueryResult> executeForResult(String query) throws ShimException {
		try {
			return SciDBHandler.execute(ci, query);
		} catch (LocalQueryExecutionException | SQLException e) {
			throw new ShimException(e.getMessage(), e);
		}
	}
	
	@Override
	public void executeNoResult(String query) throws ShimException {
		try {
			SciDBHandler.executeStatementAFL(ci,query);
		} catch (SQLException e) {
			throw new ShimException(e.getMessage(), e);
		}
	};
	
	@Override
	public void dropTableIfExists(String name) throws ShimException {
		try {
			SciDBHandler.dropArrayIfExists(ci, name);
		} catch (SQLException e) {
			throw new ShimException(e.getMessage(), e);
		}
	};

}

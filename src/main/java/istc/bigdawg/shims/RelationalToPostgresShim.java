package istc.bigdawg.shims;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.exceptions.ShimException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.operators.SQLIslandJoin;
import istc.bigdawg.islands.relational.operators.SQLIslandOperator;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;

public class RelationalToPostgresShim implements Shim {

	private PostgreSQLConnectionInfo ci;
	private PostgreSQLHandler handler = null;
	
	@Override
	public void connect(ConnectionInfo ci) throws ShimException {
		if (ci instanceof PostgreSQLConnectionInfo) {
			this.ci = (PostgreSQLConnectionInfo) ci;
			handler = new PostgreSQLHandler(this.ci);
		} else {
			throw new ShimException("Input Connection Info is not PostgreSQLConnectionInfo; rather: "+ ci.getClass().getName());
		}
	}
	
	@Override
	public void disconnect() throws ShimException {
		if (handler != null) {
			try {
				handler.close();	
			} catch (Exception e) {
				throw new ShimException(e.getMessage(), e);
			}
		}
	}
	
	@Override
	public String getSelectQuery(Operator root) throws ShimException {
		
		assert(root instanceof SQLIslandOperator);
		
		OperatorQueryGenerator gen = new PostgreSQLQueryGenerator();
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
		
		OperatorQueryGenerator gen = new PostgreSQLQueryGenerator();
		gen.configure(true, stopsAtJoin);
		try {
			root.accept(gen);	
			return gen.generateSelectIntoStatementForExecutionTree(dest);
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
	}
	
	@Override
	public Pair<Operator, String> getQueryForNonMigratingSegment(Operator operator, boolean isSelect) throws ShimException {
		
		assert(operator instanceof SQLIslandOperator);
		
		OperatorQueryGenerator gen = new PostgreSQLQueryGenerator();
		gen.configure(true, false);
		try {
			operator.accept(gen);	
			return gen.generateStatementForPresentNonMigratingSegment(operator, isSelect);
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
	};

	@Override
	public List<String> getJoinPredicate(Join join) throws ShimException {
		
		assert(join instanceof SQLIslandJoin);
		
		OperatorQueryGenerator gen = new PostgreSQLQueryGenerator();
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
			return handler.execute(query);
		} catch (LocalQueryExecutionException e) {
			try {
				handler.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			throw new ShimException(e.getLocalizedMessage());
		}
	}
	
	@Override
	public void executeNoResult(String query) throws ShimException {
		try {
			handler.executeStatementPostgreSQL(query);
		} catch (SQLException e) {
			throw new ShimException(e.getMessage(), e);
		}
	};
	
	@Override
	public void dropTableIfExists(String name) throws ShimException {
		try {
			handler.dropDataSetIfExists(name);
		} catch (SQLException e) {
			throw new ShimException(e.getMessage(), e);
		}
	};

}

package istc.bigdawg.shims;

import java.util.List;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;
import istc.bigdawg.accumulo.AccumuloExecutionEngine;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.text.operators.TextOperator;
import istc.bigdawg.islands.text.operators.TextScan;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;

public class TextToAccumuloShim implements Shim {

	private AccumuloConnectionInfo ci = null;
	private AccumuloExecutionEngine handler = null;
	
	@Override
	public void connect(ConnectionInfo ci) throws ShimException {
		if (ci instanceof PostgreSQLConnectionInfo) {
			this.ci = (AccumuloConnectionInfo) ci;
			try {
				handler = new AccumuloExecutionEngine(this.ci);
			} catch (BigDawgException | AccumuloException | AccumuloSecurityException e) {
				throw new ShimException(e.getMessage(), e);
			}
		} else {
			throw new ShimException("Input Connection Info is not PostgreSQLConnectionInfo; rather: "+ ci.getClass().getName());
		}
	}

	@Override
	public void disconnect() throws ShimException {
		//// intentionally left blank
	}

	@Override
	public String getSelectQuery(Operator root) throws ShimException {
		
		assert(root instanceof TextOperator);
		assert(root instanceof TextScan);

		try {
			return root.getSubTreeToken();
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
	}

	@Override
	public String getSelectIntoQuery(Operator root, String dest, boolean stopsAtJoin) throws ShimException {
		try {
			AccumuloExecutionEngine.addExecutionTree(AccumuloExecutionEngine.AccumuloTempTableCommandPrefix+root.getSubTreeToken(), null);
			return root.getSubTreeToken();
		} catch (Exception e) {
			throw new ShimException(e.getMessage(), e);
		}
	}

	@Override
	public Pair<Operator, String> getQueryForNonMigratingSegment(Operator operator, boolean isSelect)
			throws ShimException {
		throw new ShimException("TextToAccumulo's getQueryForNonMigratingSegment is not implemented");
	}
	
	@Override
	public List<String> getJoinPredicate(Join join) throws ShimException {
		throw new ShimException("TextToAccumulo's getJoinPredicate is not implemented");
	};

	@Override
	public Optional<QueryResult> executeForResult(String query) throws ShimException {
		try {
			return handler.execute(query);
		} catch (LocalQueryExecutionException e) {
			throw new ShimException(e.getMessage(), e);
		}
	}

	@Override
	public void executeNoResult(String query) throws ShimException {
		throw new ShimException("TextToAccumulo's executeNoResult is not implemented");
	}

	@Override
	public void dropTableIfExists(String name) throws ShimException {
		try {
			handler.dropDataSetIfExists(name);
		} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			throw new ShimException(e.getMessage(), e);
		}
	}

}

package istc.bigdawg.shims;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.islands.api.operators.ApiOperator;
import istc.bigdawg.islands.api.operators.ApiSeqScan;
import istc.bigdawg.rest.RESTHandler;
import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.exceptions.ShimException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.operators.SQLIslandOperator;
import istc.bigdawg.query.ConnectionInfo;

public class ApiToRESTShim implements Shim {

    public final static String INTO_SPECIFIER = "BIGDAWG_API_REST_INTO";
    public final static String INTO_DELIMITER = "::::";
    private RESTConnectionInfo connectionInfo;
    private RESTHandler handler = null;

    @Override
    public void connect(ConnectionInfo connectionInfo) throws ShimException {
        if (connectionInfo instanceof RESTConnectionInfo) {
            this.connectionInfo = (RESTConnectionInfo) connectionInfo;
            handler = new RESTHandler(this.connectionInfo);
        } else {
            throw new ShimException("Input Connection Info is not RESTConnectionInfo; rather: "+ connectionInfo.getClass().getName());
        }
    }

    @Override
    public void disconnect() throws ShimException {
    }

    @Override
    public String getSelectQuery(Operator root) throws ShimException {
        assert(root instanceof ApiOperator);
        assert(root instanceof ApiSeqScan);

        OperatorQueryGenerator gen = new RESTQueryGenerator();
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
        // Have to hack this for intra island cast, as it seems it wants to create a
        //  temp table every time even if it's not needed
        return INTO_SPECIFIER + INTO_DELIMITER + dest + INTO_DELIMITER + this.getSelectQuery(root);
    }

    @Override
    public Pair<Operator, String> getQueryForNonMigratingSegment(Operator operator, boolean isSelect)
            throws ShimException {
        throw new ShimException("ApiToREST's getQueryForNonMigratingSegment is not implemented");
    }

    @Override
    public List<String> getJoinPredicate(Join join) throws ShimException {
        throw new ShimException("ApiToREST's getJoinPredicate is not implemented");
    };

    @Override
    public Optional<QueryResult> executeForResult(String query) throws ShimException {
        return handler.execute(query);
    }

    @Override
    public void executeNoResult(String query) throws ShimException {
        throw new ShimException("ApiToREST's executeNoResult is not implemented");
    };

    @Override
    public void dropTableIfExists(String name) throws ShimException {
        throw new ShimException("ApiToREST's dropTableIfExists is not implemented");
    };

}

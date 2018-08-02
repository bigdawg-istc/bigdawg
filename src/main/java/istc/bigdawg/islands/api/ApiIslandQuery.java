package istc.bigdawg.islands.api;

import java.util.*;

import istc.bigdawg.api.AbstractApiConnectionInfo;
import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.islands.AbstractNonRelationalIslandQuery;
import istc.bigdawg.islands.api.operators.ApiSeqScan;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.rest.RESTConnectionInfo;
import org.apache.log4j.Logger;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.islands.Island;
import istc.bigdawg.islands.IslandAndCastResolver;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.islands.relational.RelationalIslandQuery;
import istc.bigdawg.signature.Signature;

public class ApiIslandQuery extends AbstractNonRelationalIslandQuery {

    ApiIslandQuery(String islandQuery, String name, Map<String, String> transitionSchemas)
            throws Exception {
        super(Scope.API, islandQuery, name, transitionSchemas);
    }

    @Override
    public List<QueryExecutionPlan> getAllQEPs(boolean isSelect) throws Exception {

        List<QueryExecutionPlan> qepl = new ArrayList<>();

        logger.info(String.format("RemainderPermuations, from getAllQEPs: %s\n", remainderPermutations));

        for (int i = 0; i < remainderPermutations.size(); i++ ){
            QueryExecutionPlan qep = new QueryExecutionPlan(getSourceScope());
            ExecutionNodeFactory.addNodesAndEdgesNew(qep, remainderPermutations.get(i), remainderLoc, queryContainer, isSelect, name);
            qepl.add(qep);
            ConnectionInfo connectionInfo = qep.getTerminalTableNode().getEngine();
            ApiSeqScan apiSeqScan = (ApiSeqScan) remainderPermutations.get(0);
            this.verifyQueryParameters((AbstractApiConnectionInfo) connectionInfo, apiSeqScan.getQueryParameters());
        }

        return qepl;
    }

    private void verifyQueryParameters(AbstractApiConnectionInfo abstractApiConnectionInfo, Map<String, String> queryParameters) throws ApiException  {
        List<String> requiredParams = abstractApiConnectionInfo.getRequiredParams();
        if (requiredParams != null && !requiredParams.isEmpty()) {
            StringJoiner missing = new StringJoiner(", ");
            for (String requiredParam: requiredParams) {
                if (!queryParameters.containsKey(requiredParam)) {
                    missing.add(requiredParam);
                }
            }
            if (missing.length() > 0) {
                throw new ApiException("Missing required query parameter(s): " + missing.toString());
            }
        }
    }
}

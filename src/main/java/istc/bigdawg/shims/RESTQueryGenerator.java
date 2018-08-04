package istc.bigdawg.shims;

import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.islands.api.operators.ApiSeqScan;
import istc.bigdawg.islands.operators.*;
import istc.bigdawg.rest.HttpMethod;
import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.rest.URLUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class RESTQueryGenerator implements OperatorQueryGenerator {
    private boolean stopAtJoin = false;
    private boolean isRoot = true;
    private RESTConnectionInfo restConnectionInfo;
    Operator root = null;

    RESTQueryGenerator(RESTConnectionInfo restConnectionInfo) {
        this.restConnectionInfo = restConnectionInfo;
    }

    @Override
    public void configure(boolean isRoot, boolean stopAtJoin) {
        this.stopAtJoin = stopAtJoin;
        this.isRoot = isRoot;
    }

    @Override
    public void reset(boolean isRoot, boolean stopAtJoin) {
        this.stopAtJoin = stopAtJoin;
        this.isRoot = isRoot;
        root = null;
    }

    private void saveRoot(Operator o) {
        if (!this.isRoot)
            return;
        this.root = o;
        this.isRoot = false;
    }

    @Override
    public String generateStatementString() throws Exception {
        if (this.root == null) {
            throw new Exception("No root");
        }
        assert(this.root instanceof ApiSeqScan);
        ApiSeqScan apiSeqScan = (ApiSeqScan) this.root;
        Map<String, String> queryParameters = apiSeqScan.getQueryParameters();
        Map<String, Object> statement = new HashMap<>();
        if (queryParameters != null) {
            statement.put("query", queryParameters);
        }
        String queryRaw = apiSeqScan.getQueryRaw();
        if (queryRaw != null) {
            statement.put("query-raw", apiSeqScan.getQueryRaw());
        }
        String resultKey = apiSeqScan.getResultKey();
        if (resultKey != null) {
            statement.put("result-key", resultKey);
        }
        String tag = apiSeqScan.getTag();
        if (tag != null) {
            statement.put("tag", apiSeqScan.getTag());
        }
        return (JSONObject.toJSONString(statement));
    }

    @Override
    public void visit(Limit limit) throws Exception {
        throw new Exception("Unsupported Operator for REST: Limit");
    }

    @Override
    public void visit(Merge merge) throws Exception {
        throw new Exception("Unsupported Operator for REST: Merge");
    }

    @Override
    public void visit(WindowAggregate operator) throws Exception {
        throw new Exception("Unsupported Operator for REST: WindowAggregate");
    }

    @Override
    public void visit(CommonTableExpressionScan cte) throws Exception {
        throw new Exception("Unsupported Operator for REST: CTE");
    }

    @Override
    public void visit(SeqScan operator) throws Exception {
        saveRoot(operator);
    }

    @Override
    public void visit(Aggregate aggregateOp) throws Exception {
        throw new Exception("Unsupported Operator for REST: Aggregate");
    }

    @Override
    public void visit(Distinct distinct) throws Exception {
        throw new Exception("Unsupported Operator for REST: Distinct");
    }

    @Override
    public void visit(Scan scanOp) throws Exception {
        throw new Exception("Unsupported Operator for REST: Scan");
    }

    @Override
    public void visit(Sort sortOp) throws Exception {
        throw new Exception("Unsupported Operator for REST: Sort");
    }

    @Override
    public void visit(Operator operator) throws Exception {
        throw new Exception("Unsupported Operator for REST: Operator");
    }

    @Override
    public void visit(Join joinOp) throws Exception {
        throw new Exception("Unsupported Operator for REST: Join");
    }

    @Override
    public Pair<Operator, String> generateStatementForPresentNonMigratingSegment(Operator operator, boolean isSelect)
            throws Exception {
        throw new Exception("Unsupported for REST: generateStatementForPresentNonMigratingSegment");
    }

    @Override
    public String generateSelectIntoStatementForExecutionTree(String destinationTable) throws Exception {
        throw new Exception("Unsupported for REST: generateSelectIntoStatementForExecutionTree");
    }

    @Override
    public String generateCreateStatementLocally(Operator op, String name) throws Exception {
        throw new Exception("Unsupported for REST: generateCreateStatementLocally");
    }

    @Override
    public List<String> getJoinPredicateObjectsForBinaryExecutionNode(Join join) throws Exception {
        throw new Exception("Unsupported for REST: getJoinPredicateObjectsForBinaryExecutionNode");
    }
}
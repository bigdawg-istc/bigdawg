package istc.bigdawg.islands.api.operators;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Range;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.shims.OperatorQueryGenerator;
import org.json.JSONObject;

public class ApiSeqScan extends ApiOperator implements SeqScan {

    public static enum EncodingMode { URL, JSON };

    private Map<String, String> queryParameters;
    private String urlStr;
    private String queryRaw;

    public ApiSeqScan(String urlStr, Map<String, String> queryParameters) {
        super();
        this.urlStr = urlStr;
        this.queryParameters = queryParameters;
    }

    public void setQueryRaw(String queryRaw) {
        this.queryRaw = queryRaw;
    }

    public Map<String, String> getQueryParameters() {
        return this.queryParameters;
    }

    public String getQueryRaw() {
        return this.queryRaw;
    }

    @Override
    public Operator duplicate(boolean addChild) throws IslandException {
        ApiSeqScan apiSeqScan = new ApiSeqScan(urlStr, queryParameters);
        apiSeqScan.setQueryRaw(this.queryRaw);
        return apiSeqScan;
    }

    @Override
    public Map<String, String> getDataObjectAliasesOrNames() throws IslandException {
        Map<String, String> result = new HashMap<>();
        result.put(urlStr, urlStr);
        return result;
    }

    @Override
    public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws IslandException {
        // intentionally left blank
    }

    @Override
    public String getTreeRepresentation(boolean isRoot) throws IslandException {
        if (queryParameters != null) {
            return String.format("ApiSeqScan, %s, %s)", urlStr, (new JSONObject(queryParameters)).toString());
        }
        if (queryRaw != null) {
            return String.format("ApiSeqScan, %s, %s)", urlStr, queryRaw);
        }
        return String.format("ApiSeqScan, %s)", urlStr);
    }

    @Override
    public Map<String, Set<String>> getObjectToExpressionMappingForSignature() throws IslandException {
        return new HashMap<>();
    }

    @Override
    public void accept(OperatorQueryGenerator operatorQueryGenerator) throws Exception {
        operatorQueryGenerator.visit(this);
    }

    @Override
    public String getSourceTableName() {
        return this.urlStr;
    }

    @Override
    public void setSourceTableName(String srcTableName) {
        this.urlStr = srcTableName;
    }

    @Override
    public String generateRelevantJoinPredicate() throws IslandException {
        return null;
    }

    @Override
    public String getFullyQualifiedName() {
        return this.getSourceTableName();
    }

}

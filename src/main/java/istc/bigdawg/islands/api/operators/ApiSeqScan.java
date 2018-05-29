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

    private Map<String, String> queryParameters;
    private String apiName;
    private String endpoint;
    private String queryRaw;

    public ApiSeqScan(String apiName, String endpoint, Map<String, String> queryParameters) {
        super();
        this.apiName = apiName;
        this.endpoint = endpoint;
        this.queryParameters = queryParameters;
    }

    public void setQueryRaw(String queryRaw) {
        this.queryRaw = queryRaw;
    }

    public String getApiName() {
        return apiName;
    }

    public void setApiName(String apiName) {
        this.apiName = apiName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Map<String, String> getQueryParameters() {
        return this.queryParameters;
    }

    public String getQueryRaw() {
        return this.queryRaw;
    }

    @Override
    public Operator duplicate(boolean addChild) throws IslandException {
        ApiSeqScan apiSeqScan = new ApiSeqScan(apiName, endpoint, queryParameters);
        apiSeqScan.setQueryRaw(this.queryRaw);
        return apiSeqScan;
    }

    @Override
    public Map<String, String> getDataObjectAliasesOrNames() throws IslandException {
        Map<String, String> result = new HashMap<>();
        result.put(endpoint, endpoint);
        return result;
    }

    @Override
    public void removeCTEEntriesFromObjectToExpressionMapping(Map<String, Set<String>> entry) throws IslandException {
        // intentionally left blank
    }

    @Override
    public String getTreeRepresentation(boolean isRoot) throws IslandException {
        if (queryParameters != null) {
            return String.format("ApiSeqScan, %s, %s, %s)", apiName, endpoint, (new JSONObject(queryParameters)).toString());
        }
        if (queryRaw != null) {
            return String.format("ApiSeqScan, %s, %s, %s)", apiName, endpoint, queryRaw);
        }
        return String.format("ApiSeqScan, %s, %s)", apiName, endpoint);
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
        return apiName + "/" + endpoint;
    }

    @Override
    public void setSourceTableName(String srcTableName) {
        String[] parts = srcTableName.split("/");
        assert(parts.length == 2);

        this.apiName = parts[0];
        this.endpoint = parts[1];
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

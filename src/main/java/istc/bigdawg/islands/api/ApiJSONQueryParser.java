package istc.bigdawg.islands.api;

import java.util.*;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.islands.api.operators.ApiSeqScan;
import istc.bigdawg.query.AbstractJSONQueryParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import istc.bigdawg.islands.operators.Operator;

public class ApiJSONQueryParser extends AbstractJSONQueryParser {

    public OperatorTypes getOperatorType(JSONObject parsedObject, String input) throws ParseException {
        if (parsedObject.get("name") == null || !(parsedObject.get("name") instanceof String))
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "ApiJSONQueryParser parsing error: cannot identify api name. query: "+input);
        if (parsedObject.get("endpoint") == null || !(parsedObject.get("endpoint") instanceof String))
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "ApiJSONQueryParser parsing error: cannot identify api endpoint. query: "+input);

        return OperatorTypes.SCAN;
    };

    protected Operator getScan(JSONObject parsedObject) throws ParseException {
        StringJoiner missingParameters = new StringJoiner(", ");
        Object temp = getObjectByType(parsedObject.get("name"), String.class);
        String apiName = null;
        if (temp == null) {
            missingParameters.add("name");
        }
        else {
            apiName = (String)temp;
        }
        temp = getObjectByType(parsedObject.get("endpoint"), String.class);
        String endpoint = null;
        if (temp == null) {
            missingParameters.add("name");
        }
        else {
            endpoint = (String)temp;
        }

        if (missingParameters.length() > 0) {
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "Missing bdapi query arguments: " + missingParameters.toString());
        }
        JSONObject queryObject = (JSONObject)getObjectByType(parsedObject.get("query"), JSONObject.class);
        HashMap<String, String> query = null;

        if (queryObject != null) {
            query = new HashMap<>();
            for (Object key: queryObject.keySet()) {
                String keyStr = (String)getObjectByType(key, String.class);
                String value = (String)getObjectByType(queryObject.get(keyStr), String.class);
                query.put(keyStr, value);
            }

        }
        List<String> objs;
        try {
            objs = CatalogViewer.getObjectNames(apiName, endpoint);
            if (objs.size() > 1) {
                throw new Exception("Expected exactly one result from querying catalog on " + apiName + ", " + endpoint + " - instead got: " + String.valueOf(objs.size()));
            }
            if (objs.size() < 1) {
                throw new Exception("Expected a result from querying catalog on " + apiName + ", " + endpoint + " - instead got: " + String.valueOf(objs.size()));
            }
        }
        catch (Exception e) {
            throw new ParseException(ParseException.ERROR_UNEXPECTED_EXCEPTION, e);
        }
        ApiSeqScan apiSeqScan = new ApiSeqScan(objs.get(0), query);
        if (parsedObject.containsKey("query") && parsedObject.containsKey("query-raw")) {
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "Can't do both a 'query' and 'query-raw' in a bdapi call");
        }

        if (parsedObject.containsKey("query-raw")) {
            if (queryObject != null) {
                throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "ApiJSONQueryParser - unexpected 'query-raw' when 'query' present");
            }
            String queryRaw = (String) getObjectByType(parsedObject.get("query-raw"), String.class);
            apiSeqScan.setQueryRaw(queryRaw);
        }

        // Ability to override result key
        if (parsedObject.containsKey("result-key")) {
            String resultKey = (String) getObjectByType(parsedObject.get("result-key"), String.class);
            apiSeqScan.setResultKey(resultKey);
        }
        return apiSeqScan;
    }
}

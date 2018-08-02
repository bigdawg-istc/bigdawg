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

        String apiName = (String)getObjectByType(parsedObject.get("name"), String.class);
        String endpoint = (String)getObjectByType(parsedObject.get("endpoint"), String.class);
        JSONObject queryObject = (JSONObject)getObjectByType(parsedObject.get("query"), JSONObject.class);
        HashMap<String, String> query = null;

        if (queryObject != null) {
            query = new HashMap<String, String>();
            for (Object key: queryObject.keySet()) {
                String keyStr = (String)getObjectByType(key, String.class);
                String value = (String)getObjectByType(queryObject.get(keyStr), String.class);
                query.put(keyStr, value);
            }

        }
        List<String> objs;
        try {
            objs = CatalogViewer.getObjectNames(apiName, endpoint);
            if (objs.size() != 1) {
                throw new Exception("Expected exactly one result from querying catalog on " + apiName + ", " + endpoint + " - instead got: " + String.valueOf(objs.size()));
            }
        }
        catch (Exception e) {
            throw new ParseException(ParseException.ERROR_UNEXPECTED_EXCEPTION, e);
        }
        ApiSeqScan apiSeqScan = new ApiSeqScan(objs.get(0), query);
        if (parsedObject.containsKey("query-raw")) {
            if (queryObject != null) {
                throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "ApiJSONQueryParser - unexpected 'query-raw' when 'query' present");
            }
            String queryRaw = (String) getObjectByType(parsedObject.get("query-raw"), String.class);
            apiSeqScan.setQueryRaw(queryRaw);
        }
        return apiSeqScan;
    }
}

package istc.bigdawg.islands.api;

import java.util.*;

import istc.bigdawg.islands.api.operators.ApiSeqScan;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.text.operators.TextScan;

public class ApiJSONQueryParser {

    private JSONParser parser;

    public ApiJSONQueryParser() {
        parser = new JSONParser();
    }

    public Operator parse(String input) throws ParseException {
        JSONObject parsedObject = (JSONObject) parser.parse(input.replaceAll("[']", "\""));

        if (parsedObject.get("name") == null || !(parsedObject.get("name") instanceof String))
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "ApiJSONQueryParser parsing error: cannot identify api name. query: "+input);
        if (parsedObject.get("endpoint") == null || !(parsedObject.get("endpoint") instanceof String))
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "ApiJSONQueryParser parsing error: cannot identify api endpoint. query: "+input);

        return getSeqScan(parsedObject);
    };

    @SuppressWarnings("rawtypes")
    public static Object getObjectByType(Object objectHolder, Class clazz) throws ParseException {
        if (objectHolder == null)
            return null;
        if (! (objectHolder.getClass().equals(clazz)))
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "ApiJSONQueryParser parsing error: data type mismatch: expecting "+clazz.getName()+"; received: "+objectHolder.getClass().getName());
        return objectHolder;
    }

    private String getNonNullString(Object input) throws ParseException {
        String s = (String) getObjectByType(input, String.class);
        if (s == null) return "";
        return s;
    }

    public static void addNonNullStringToList(Object input, List<String> output) throws ParseException{
        String s = (String) getObjectByType(input, String.class);
        if (s != null) output.add(s);
    }

    private Operator getSeqScan(JSONObject parsedObject) throws ParseException {

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

        ApiSeqScan apiSeqScan = new ApiSeqScan(apiName, endpoint, query);
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

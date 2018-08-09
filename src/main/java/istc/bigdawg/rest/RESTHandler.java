package istc.bigdawg.rest;

import istc.bigdawg.BDConstants;
import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.executor.RESTQueryResult;
import istc.bigdawg.query.AbstractJSONQueryParser;
import istc.bigdawg.shims.ApiToRESTShim;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.utils.Tuple;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class RESTHandler implements ExecutorEngine, DBHandler {
    private RESTConnectionInfo restConnectionInfo;
    private static Logger log = Logger
            .getLogger(RESTHandler.class.getName());

    public RESTHandler(RESTConnectionInfo restConnectionInfo) {
        this.restConnectionInfo = restConnectionInfo;
    }

    // Use Hashtable for reentrancy concerns
    // @TODO - how to clean this out periodically?
    // @TODO - how to prevent multiple entries of the same tag name?
    private static Map<String, RESTQueryResult> restQueryResults = new Hashtable<>();


    @Override
    public Optional<QueryResult> execute(String query) {
        String bigdawgResultTag = "";
        Optional<QueryResult> queryResult;
        try {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(query);
            if (jsonObject.containsKey("tag")) {
                bigdawgResultTag = (String) jsonObject.get("tag");
            }

            String url = restConnectionInfo.getUrl();
            HttpMethod method = restConnectionInfo.getMethod();
            String postData = null;
            String queryParameters = null;
            switch(method) {
                case POST:
                    String contentType = restConnectionInfo.getContentType();
                    if (contentType == null) {
                        throw new ApiException("Null Content-Type");
                    }

                    if (jsonObject.containsKey("query-raw")) {
                        postData = (String) jsonObject.get("query-raw");
                    }
                    else if (jsonObject.containsKey("query")) {
                        Map<String, String> queryParametersMap = AbstractJSONQueryParser.jsonObjectToKeyValueString((JSONObject) jsonObject.get("query"));
                        switch (contentType) {
                            case "application/x-www-form-urlencoded":
                                postData = URLUtil.encodeParameters(queryParametersMap);
                                break;
                            case "application/json":
                                postData = JSONObject.toJSONString(queryParametersMap);
                                break;
                            default:
                                throw new ApiException("Unknown Content-Type: " + contentType);
                        }
                    }
                    queryParameters = restConnectionInfo.getFinalQueryParameters(null);
                    break;
                case GET:
                    String queryString = null;
                    if (jsonObject.containsKey("query-raw")) {
                        queryString = (String) jsonObject.get("query-raw");
                    }
                    else if (jsonObject.containsKey("query")) {
                        Map<String, String> queryParametersMap = AbstractJSONQueryParser.jsonObjectToKeyValueString((JSONObject) jsonObject.get("query"));
                        queryString = URLUtil.encodeParameters(queryParametersMap);
                    }
                    queryParameters = restConnectionInfo.getFinalQueryParameters(queryString);
                    break;
                default:
                    throw new ApiException("Unknown/Unsupported HttpMethod: " + method);
            }
            if (queryParameters != null) {
                url = URLUtil.appendQueryParameters(url, queryParameters);
            }
            Map<String, String> headers = restConnectionInfo.getHeaders(queryParameters);

            // @TODO Connect / read timeout could be parameterized either in query or in connection parameters, or both
            URLUtil.FetchResult fetchResult = URLUtil.fetch(url, method, headers, postData, restConnectionInfo.getConnectTimeout(), restConnectionInfo.getReadTimeout());
            String resultKey = null;
            if (jsonObject.containsKey("result-key")) {
                resultKey = (String) jsonObject.get("result-key");
            }

            RESTQueryResult restQueryResult = this.parseResult(fetchResult, resultKey);
            if (restQueryResult == null) {
                queryResult = Optional.empty();
            }
            else {
                RESTHandler.restQueryResults.put(bigdawgResultTag, restQueryResult);
                queryResult = Optional.of(restQueryResult);
            }
        }
        catch (Exception e) {
            RESTHandler.log.error("Error executing REST query", e); // how to bubble this up?
            queryResult = Optional.empty();
        }
        return queryResult;
    }

    /**
     * Parses the result from a fetch
     * @param fetchResult Result structure from teh fetch
     * @return List of results as parsed
     * @throws ApiException when there's a problem parsing
     */
    private RESTQueryResult parseResult(URLUtil.FetchResult fetchResult, String resultKey) throws ApiException {
        if (resultKey == null) {
            resultKey = restConnectionInfo.getResultKey();
        }
        RESTHandler.log.info("RESTHandler - result_key" + String.valueOf(resultKey == null ? resultKey : "<null>"));

        try {
            // @TODO support other content types e.g. text/csv or tsv or even maybe xml?
            JSONParser jsonParser = new JSONParser();
            if (!URLUtil.headersContain(fetchResult.responseHeaders, "content-type", URLUtil.HeaderMatch.jsonHeaderMatchTypes, ";")) {
                throw new ApiException("Unsupported content type: " + fetchResult.responseHeaders.get("content-type").get(0));
            }
            Object object = jsonParser.parse(fetchResult.response);
            RESTHandler.log.info("RESTHandler - parsing result: " + String.valueOf(fetchResult.response));

            return this.parseJSONResult(resultKey, object);
        } catch (ParseException e) {
            throw new ApiException("Exception encountered trying to parse the result: " + e.toString());
        }
    }

    private RESTQueryResult parseJSONArray(JSONArray jsonArray) {
        // Tuple3 (colname, type, nullable)
        List<Tuple.Tuple3<String, String, Boolean>> headers = new ArrayList<>();
        Map<String, Integer> headerNames = new HashMap<>();
        List<String> rows = new ArrayList<>();
        List<Map<String, Object>> rowsWithHeadings = new ArrayList<>();
        for (Object o : jsonArray) {
            String row = "";
            Map<String, Object> rowWithHeadings = new HashMap<>();
            if (o == null) {
                int idx = 0;
                if (headerNames.containsKey("col1")) {
                    idx = headerNames.get("col1");
                    Tuple.Tuple3<String, String, Boolean> tuple3 = headers.get(idx);
                    if (!tuple3.getT3()) {
                        headers.set(idx, new Tuple.Tuple3<String, String, Boolean>(tuple3.getT1(), tuple3.getT2(), true));
                    }
                }
                else {
                    headers.add(new Tuple.Tuple3<>("col1", "text", true));
                }

                rowWithHeadings.put("col1", null);
                row = "";
            } else if (o.getClass() == JSONArray.class) {
                row = getFromJSONArray(headers, headerNames, rowWithHeadings, (JSONArray) o);
            } else if (o.getClass() == JSONObject.class) {
                row = getFromJSONObject(headers, headerNames, rowWithHeadings, (JSONObject) o);
            }
            else {
                final Tuple.Tuple2<String, Boolean> headerInfo = determineHeaderTypeNullable(o);
                final String headerType = headerInfo.getT1();
                final boolean nullable = headerInfo.getT2();

                if (!headerNames.containsKey("col1")){
                    headerNames.put("col1", headers.size());
                    headers.add(new Tuple.Tuple3<String, String, Boolean>("col1", headerType, nullable));
                }
                final int headersSize = headers.size();
                for (int i = 0; i < headersSize; i++) {
                    Tuple.Tuple3<String, String, Boolean> header = headers.get(i);
                    if (header.getT1().equals("col1")) {
                        if (nullable && !header.getT3()) {
                            headers.set(i, new Tuple.Tuple3<String, String, Boolean>("col1", header.getT2(), true));
                        }
                        rowWithHeadings.put("col1", o);
                    }
                    else {
                        if (!header.getT3()) {
                            headers.set(i, new Tuple.Tuple3<String, String, Boolean>("col1", header.getT2(), true));
                        }
                    }
                }
            }
            rows.add(row);
            rowsWithHeadings.add(rowWithHeadings);
        }
        return new RESTQueryResult(headers, rows, rowsWithHeadings, restConnectionInfo);
    }

    private String getFromJSONObject(List<Tuple.Tuple3<String, String, Boolean>> headers,
                                     Map<String, Integer> headerNames,
                                     Map<String, Object> rowWithHeaders,
                                     JSONObject jsonObject) {
        List<Object> row = new ArrayList<>();

        for (Object key : jsonObject.keySet()) {
            String keyStr = String.valueOf(key);

            Object obj = jsonObject.get(key);
            final Tuple.Tuple2<String, Boolean> headerInfo = determineHeaderTypeNullable(obj);
            final String headerType = headerInfo.getT1();
            final boolean nullable = headerInfo.getT2();

            if (headerNames.containsKey(keyStr)) {
                final int idx = headerNames.get(keyStr);
                final Tuple.Tuple3<String, String, Boolean> tuple3 = headers.get(idx);
                if (nullable) {
                    if (!tuple3.getT3()) {
                        headers.set(idx, new Tuple.Tuple3<String, String, Boolean>(tuple3.getT1(), tuple3.getT2(), true));
                    }
                } else {
                    final String t2 = tuple3.getT2();
                    if (!t2.equals("json") && !t2.equals(headerType)) {
                        headers.set(idx, new Tuple.Tuple3<String, String, Boolean>(tuple3.getT1(), "json", true));
                    }
                }
            } else {
                headerNames.put(keyStr, headers.size());
                headers.add(new Tuple.Tuple3<String, String, Boolean>(keyStr, headerType, nullable));
            }
        }

        final int headersSize = headers.size();
        for (int i = 0; i < headersSize; i++) {
            Tuple.Tuple3<String, String, Boolean> tuple3 = headers.get(i);
            final String headerName = tuple3.getT1();
            if (jsonObject.containsKey(headerName)) {
                final Object obj = jsonObject.get(headerName);
                rowWithHeaders.put(headerName, obj);
            }
            else {
                if (!tuple3.getT3()) {
                    headers.set(i, new Tuple.Tuple3<String, String, Boolean>(headerName, tuple3.getT2(), true));
                }
            }
        }

        return jsonObject.toString();
    }

    private String getFromJSONArray(List<Tuple.Tuple3<String, String, Boolean>> headers, Map<String, Integer> headerNames, Map<String, Object> rowWithHeadings, JSONArray jsonArray) {
        final int jsonSize = jsonArray.size();
        List<Object> row = new ArrayList<>();
        for (int i = 0 ; i < jsonSize; i++) {
            final String colName = "col" + String.valueOf(i);
            final Object obj = jsonArray.get(i);
            if (!headerNames.containsKey(colName)) {
                final Tuple.Tuple2<String, Boolean> headerInfo = determineHeaderTypeNullable(obj);
                final String headerType = headerInfo.getT1();
                final boolean nullable = headerInfo.getT2();
                headers.add(new Tuple.Tuple3<String, String, Boolean>(colName, headerType, nullable));
                headerNames.put(colName, headers.size() - 1);
            }
        }

        int j = 0;
        final int headersSize = headers.size();
        for (int i = 0; i < headersSize; i++) {
            final Tuple.Tuple3<String, String, Boolean> header = headers.get(i);
            if (j < jsonSize) {
                final String colName = "col" + String.valueOf(j);
                if (header.getT1().equals(colName)) {
                    final Object obj = jsonArray.get(j);
                    j++;
                    final Tuple.Tuple2<String, Boolean> headerInfo = determineHeaderTypeNullable(obj);
                    final String headerType = headerInfo.getT1();
                    final boolean nullable = headerInfo.getT2();
                    final String t2 = header.getT2();
                    if (nullable) {
                        if (!header.getT3()) {
                            headers.set(i, new Tuple.Tuple3<>(header.getT1(), header.getT2(), true));
                        }
                    }
                    else if (!t2.equals("json") && !t2.equals(headerType)) {
                        headers.set(i, new Tuple.Tuple3<>(header.getT1(), "json", header.getT3()));
                    }
                    rowWithHeadings.put(colName, obj);
                }
            }
            else {
                if (!header.getT3()) {
                    headers.set(i, new Tuple.Tuple3<>(header.getT1(), header.getT2(), true));
                }
            }
        }
        return jsonArray.toString();
    }

    /**
     * Determines what the header type should be given the object passed in, and whether it should be nullable or not
     * @param obj object to examine
     * @return Tuple.Tuple2 of the header type (String), and whether it should be nullable (Boolean)
     */
    private Tuple.Tuple2<String, Boolean> determineHeaderTypeNullable(Object obj) {
        if (obj == null){
            return new Tuple.Tuple2<String, Boolean>("json", true);
        }
        else if (obj.getClass() == JSONObject.class || obj.getClass() == JSONArray.class) {
            return new Tuple.Tuple2<String, Boolean>("json", false);
        }
        else if (obj.getClass() == Integer.class) {
            return new Tuple.Tuple2<String, Boolean>("integer", false);
        }
        else if (obj.getClass() == Long.class) {
            return new Tuple.Tuple2<String, Boolean>("bigint", false);
        }
        else if (obj.getClass() == Double.class) {
            return new Tuple.Tuple2<String, Boolean>("double precision", false);
        }
        else if (obj.getClass() == Boolean.class) {
            return new Tuple.Tuple2<String, Boolean>("boolean", false);
        }
        return new Tuple.Tuple2<String, Boolean>("text", false);
    }


    /**
     * Parses a json result
     * @param resultKey Index into the json object
     * @param object Object to search
     * @throws ApiException when something goes wrong
     */
    private RESTQueryResult parseJSONResult(String resultKey, Object object) throws ApiException {
        if (object == null) {
            return null;
        }

        if (object.getClass() == JSONArray.class) {
            if (resultKey != null && resultKey.length() > 0) {
                throw new ApiException("Response is a list, but expected an object due to resultKey of '" + resultKey + "'");
            }
            return parseJSONArray((JSONArray) object);
        }


        if (object.getClass() != JSONObject.class) {
            if (resultKey != null && resultKey.length() > 0) {
                throw new ApiException("Response is a JSONValue: " + object.toString() + ", but expected an object due to resultKey of '" + resultKey + "'");
            }

            return this.getBasicRESTQueryResult(object);
        }

        JSONObject jsonObject = (JSONObject) object;
        if (resultKey == null || resultKey.length() == 0) {
            List<Tuple.Tuple3<String, String, Boolean>> headers = new ArrayList<>();
            Map<String, Integer> headerNames = new HashMap<>();
            List<Map<String, Object>> rowsWithHeaders = new ArrayList<>();
            Map<String, Object> rowWithHeaders = new HashMap<>();
            String row = getFromJSONObject(headers, headerNames, rowWithHeaders, jsonObject);
            List<String> rows = new ArrayList<>();
            rows.add(row);
            rowsWithHeaders.add(rowWithHeaders);
            return new RESTQueryResult(headers, rows, rowsWithHeaders, restConnectionInfo);
        }

        // Determine if the index key is nested
        if (!jsonObject.containsKey(resultKey)) { // nested key? e.g. something.something_else
            int pos;
            if ((pos = resultKey.indexOf('.')) > 0 && pos < resultKey.length() - 1) {
                String key = resultKey.substring(0, pos);
                String rest = resultKey.substring(pos + 1);
                if (jsonObject.containsKey(key)) {
                    return this.parseJSONResult(rest, jsonObject.get(key));
                }
            }
            return null;
        }

        Object result = jsonObject.get(resultKey);
        if (result == null || result.getClass() != JSONArray.class) {
            return this.getBasicRESTQueryResult(result);
        }

        final JSONArray jsonArray = (JSONArray) result;
        return parseJSONArray(jsonArray);
    }

    /**
     * Handles a basic result
     * @param object
     * @return
     */
    private RESTQueryResult getBasicRESTQueryResult(@Nullable Object object) {
        if (object == null) {
            return null;
        }

        List<String> rows = new ArrayList<>();
        String row = object.toString();
        rows.add(row);
        List<Map<String, Object>> rowsWithHeaders = new ArrayList<>();
        Map<String, Object> rowWithHeaders = new HashMap<>();
        rowWithHeaders.put("col1", object);
        rowsWithHeaders.add(rowWithHeaders);
        Tuple.Tuple2<String, Boolean> tuple2 = determineHeaderTypeNullable(object);
        Tuple.Tuple3<String, String, Boolean> header = new Tuple.Tuple3<>("col1", tuple2.getT1(), tuple2.getT2());
        List<Tuple.Tuple3<String, String, Boolean>> headers = new ArrayList<>();
        headers.add(header);
        return new RESTQueryResult(headers, rows, rowsWithHeaders, restConnectionInfo);
    }

    @Override
    public void dropDataSetIfExists(String dataSetName) {
        // Not implemented
        assert(false);
    }



    /**
     *
     * @param queryString
     *            the query to be executed
     * @return result of the query
     */
    @Override
    public Response executeQuery(String queryString) {
        Optional<QueryResult> result = this.execute(queryString);
        return null;
    }

    /**
     *
     * @return The name of the shim in which the handler operates.
     */
    @Override
    public BDConstants.Shim getShim() {
        return null;
    }

    /**
     *
     * @param name
     *            name of the object (table, array, etc.)
     * @return the meta data about the object (e.g. names of the attributes)
     * @throws Exception
     *             (probably a connection to the database failed).
     */
    @Override
    public ObjectMetaData getObjectMetaData(String name) throws Exception {
        final RESTQueryResult restQueryResult = RESTHandler.restQueryResults.getOrDefault(name, null);

        return new ObjectMetaData() {
            @Override
            public String getName() {
                return null;
            }

            @Override
            public List<AttributeMetaData> getAttributesOrdered() {
                List <AttributeMetaData> resultList = new ArrayList<>();
                if (restQueryResult == null) {
                    AttributeMetaData attribute = new AttributeMetaData("col1", "json", true, false);
                    resultList.add(attribute);
                    return resultList;
                }

                List<Tuple.Tuple3<String, String, Boolean>> headers = restQueryResult.getColumns();
                for(Tuple.Tuple3<String, String, Boolean> header: headers) {
                    AttributeMetaData attribute = new AttributeMetaData(header.getT1(), header.getT2(), header.getT3(), false);
                    resultList.add(attribute);
                }
                return resultList;
            }
        };
    }

    /**
     * Get a JDBC connection to a database.
     *
     * @return connection to a database
     */
    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    /**
     *
     * @param name
     *            Name of the object (table/array etc.)
     * @return true if the object with the specified name exists, false
     *         otherwise.
     */
    @Override
    public boolean existsObject(String name) throws Exception {
        return false;
    }


    /**
     * Release all the resources hold by the handler.
     */
    @Override
    public void close() throws Exception {

    }

    @Override
    public String getCsvExportDelimiter() {
        return ",";
    }

}

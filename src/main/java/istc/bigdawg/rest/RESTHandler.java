package istc.bigdawg.rest;

import istc.bigdawg.BDConstants;
import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.executor.ConstructedQueryResult;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.DBHandler;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.swing.text.html.Option;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RESTHandler implements ExecutorEngine, DBHandler {
    private RESTConnectionInfo restConnectionInfo;
    private static Logger log = Logger
            .getLogger(RESTHandler.class.getName());

    public RESTHandler(RESTConnectionInfo restConnectionInfo) {
        this.restConnectionInfo = restConnectionInfo;
    }

    @Override
    public Optional<QueryResult> execute(String query) {
        try {
            String url = restConnectionInfo.getUrl();
            HttpMethod method = restConnectionInfo.getMethod();
            Map<String, String> headers = restConnectionInfo.getHeaders(query);
            String postData = null;
            String queryParameters = null;
            switch(method) {
                case POST:
                    postData = query;
                    queryParameters = restConnectionInfo.getFinalQueryParameters(null);
                    break;
                case GET:
                    queryParameters = restConnectionInfo.getFinalQueryParameters(query);
                    break;
                default:
                    throw new ApiException("Unknown/Unsupported HttpMethod: " + method);
            }
            if (queryParameters != null) {
                url = URLUtil.appendQueryParameters(url, queryParameters);
            }

            // @TODO Connect / read timeout could be parameterized either in query or in connection parameters, or both
            URLUtil.FetchResult fetchResult = URLUtil.fetch(url, method, headers, postData, restConnectionInfo.getConnectTimeout(), restConnectionInfo.getReadTimeout());

            List<List<String>> resultLists = this.parseResult(fetchResult);
            if (resultLists == null) {
                return Optional.empty();
            }
            if (resultLists.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ConstructedQueryResult(resultLists, restConnectionInfo));
        }
        catch (Exception e) {
            RESTHandler.log.error("Error executing REST query", e);
        }
        return Optional.empty();
    }

    /**
     * Parses the result from a fetch
     * @param fetchResult Result structure from teh fetch
     * @return List of results as parsed
     * @throws ApiException when there's a problem parsing
     */
    private List<List<String>> parseResult(URLUtil.FetchResult fetchResult) throws ApiException {
        List<List<String>> resultLists = new ArrayList<>();
        String resultKey = restConnectionInfo.getResultKey();
        if (resultKey != null) {
            try {
                // @TODO support other content types e.g. text/csv or tsv or even maybe xml?
                JSONParser jsonParser = new JSONParser();
                if (!URLUtil.headersContain(fetchResult.responseHeaders, "content-type", "application/json", ";")) {
                    throw new ApiException("Unsupported content type: " + fetchResult.responseHeaders.get("content-type").get(0));
                }

                JSONObject jsonObject = (JSONObject) jsonParser.parse(fetchResult.response);
                this.parseJSONResult(resultLists, resultKey, jsonObject);
            } catch (ParseException e) {
                throw new ApiException("Exception encountered trying to parse the result: " + e.toString());
            }
        }
        else {
            List<String> row = new ArrayList<>();
            row.add(fetchResult.response);
            resultLists.add(row);
        }
        return resultLists;
    }

    /**
     * Parses a json result
     * @param resultLists result list to fill with rows
     * @param resultKey Index into the json object
     * @param jsonObject JSONObject to search via the resultKey
     * @throws ApiException when something goes wrong
     */
    private void parseJSONResult(List<List<String>> resultLists, String resultKey, JSONObject jsonObject) throws ApiException {
        if (jsonObject == null) {
            return;
        }

        if (!jsonObject.containsKey(resultKey)) { // nested key? e.g. something.something_else
            int pos;
            if ((pos = resultKey.indexOf('.')) > 0 && pos < resultKey.length() - 1) {
                String key = resultKey.substring(0, pos);
                String rest = resultKey.substring(pos + 1);
                if (jsonObject.containsKey(key)) {
                    this.parseJSONResult(resultLists, rest, (JSONObject) jsonObject.get(key));
                    return;
                }
            }
            return;
        }

        Object result = jsonObject.get(resultKey);
        if (result == null) {
            // This is a bit strange, but possible I guess
            return;
        }
        if (result.getClass() != JSONArray.class) {
            // this is worse - we should have an array here...
            throw new ApiException("Result is not a JSONArray, intead it's: " + result.getClass());
        } else {
            JSONArray jsonArray = (JSONArray) result;
            for (Object o : jsonArray) {
                if (o == null) {
                    ArrayList<String> arrayList = new ArrayList<>();
                    arrayList.add("null");
                    resultLists.add(arrayList);
                } else if (o.getClass() == JSONArray.class) {
                    ArrayList<String> row = new ArrayList<>();
                    for (Object innerO : (JSONArray) o) {
                        row.add(innerO == null ? "null" : innerO.toString()); // Do I need to "null" ??
                    }
                    resultLists.add(row);
                } else {
                    ArrayList<String> arrayList = new ArrayList<>();
                    arrayList.add(o.toString());
                    resultLists.add(arrayList);
                }
            }
        }
        log.debug("REST Array result:");
        log.debug(resultLists);
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
        return new ObjectMetaData() {
            @Override
            public String getName() {
                return null;
            }

            @Override
            public List<AttributeMetaData> getAttributesOrdered() {
                AttributeMetaData attribute = new AttributeMetaData("result", "json", false, false);
                List <AttributeMetaData> resultList = new ArrayList<>();
                resultList.add(attribute);
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

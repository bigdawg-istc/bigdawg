package istc.bigdawg.rest;

import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.executor.ConstructedQueryResult;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.DBHandler;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RESTHandler implements ExecutorEngine {
    private final static int ReadTimeout = 120000; // ms
    private final static int ConnectTimeout = 60000; // ms

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
            Map<String, String> headers = restConnectionInfo.getHeaders();
            String postData = null;
            switch(method) {
                case POST:
                    postData = query;
                    break;
                case GET:
                    url = URLUtil.appendQueryParameters(url, query);
                    break;
                default:
                    throw new ApiException("Unknown/Unsupported HttpMethod: " + method);
            }

            // @TODO Connect / read timeout could be parameterized either in query or in connection parameters, or both
            String result = URLUtil.fetch(url, method, headers, postData, ConnectTimeout, ReadTimeout);
            List<List<String>> resultLists= new ArrayList<>();
            List<String> resultList = new ArrayList<>();
            resultList.add(result);
            resultLists.add(resultList);

            return Optional.of(new ConstructedQueryResult(resultLists, restConnectionInfo));
        }
        catch (Exception e) {
            RESTHandler.log.error("Error executing REST query", e);
        }
        return Optional.empty();
    }

    @Override
    public void dropDataSetIfExists(String dataSetName) {
        // Not implemented
        assert(false);
    }
}

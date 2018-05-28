/**
 *
 */
package istc.bigdawg.api;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.islands.api.ApiQueryResult;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.AccumuloShellScriptException;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.MyriaException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.Myria.MyriaQueryResult;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryResponseTupleString;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.RunShell;

/**
 * @author Matthew J Mucklo
 * @author Adam Dziedzic (original code)
 *
 */
public class ApiHandler implements DBHandler {

    /**
     * log
     */
    private Logger log = Logger.getLogger(ApiHandler.class.getName());

    private static final String apiQueryString = "curl@@@-XGET@@@http://api.openweathermap.org/data/2.5/weather?zip=%s,us&appid=2c96050e66a94faa2eedcaa6663ab2b7";

    public static QueryResult executeApiQuery(List<String> inputs) throws IOException, InterruptedException, BigDawgException {
        String queryString;
        String zipCode = inputs.get(0);

        if (inputs.size() != 1) {
            String msg = String.format("Number of query parameters must be 1, instead it's %d", inputs.size());
            System.out.println(msg);
            throw new BigDawgException(msg);
        }
        queryString = String.format(apiQueryString, zipCode);

        System.out.printf("Query string: %s;", queryString);

        InputStream scriptResultInStream = RunShell.runApiCommand(queryString);
        String scriptResult = IOUtils.toString(scriptResultInStream, Constants.ENCODING) + '\n';

        return new ApiQueryResult(scriptResult);
    }

    /*
     * (non-Javadoc)
     *
     * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
     */
    @Override
    public Response executeQuery(String queryString) {
        String resultApi = getApiData(queryString);
        return Response.status(200).entity(resultApi).build();
    }

    public String getApiData(String query) {
        String myriaResult;
        try {
            myriaResult = ApiClient.executeGetQuery(query);
        } catch (ApiException e) {
            return e.getMessage();
        }
        QueryResponseTupleString resp = new QueryResponseTupleString("OK", 200,
                myriaResult, 1, 1, new ArrayList<String>(),
                new ArrayList<String>(), new Timestamp(0));
        ObjectMapper mapper = new ObjectMapper();
        String responseResult;
        try {
            responseResult = mapper.writeValueAsString(resp); // .replace("\\", "");
            return responseResult;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            String message = "JSON processing error for API.";
            log.error(message);
            return message;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see istc.bigdawg.query.DBHandler#getShim()
     */
    @Override
    public Shim getShim() {
        return BDConstants.Shim.API;
    }

    /*
     * (non-Javadoc)
     *
     * @see istc.bigdawg.query.DBHandler#getObjectMetaData(java.lang.String)
     */
    @Override
    public ObjectMetaData getObjectMetaData(String name) throws Exception {
        // TODO
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     *
     * @see istc.bigdawg.query.DBHandler#existsObject(java.lang.String)
     */
    @Override
    public boolean existsObject(String name) throws Exception {
        // TODO
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see istc.bigdawg.query.DBHandler#close()
     */
    @Override
    public void close() throws Exception {
        log.debug("No aciton for closing API.");
    }

    /* (non-Javadoc)
     * @see istc.bigdawg.query.DBHandler#getConnection()
     */
    @Override
    public Connection getConnection() throws SQLException {
        // TODO
        throw new UnsupportedOperationException();
    }

}

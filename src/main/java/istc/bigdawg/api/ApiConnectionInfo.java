package istc.bigdawg.api;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.*;

import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.utils.Tuple;
import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.islands.IslandAndCastResolver;
import istc.bigdawg.query.ConnectionInfo;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

public class ApiConnectionInfo implements ConnectionInfo {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String domain;
    private String port;
    private String endpoint;
    private String username;
    private String password;
    private Map<String, String> connectionParameters;

    public ApiConnectionInfo(String domain, String port,
                                  String endpoint, String usr, String pw, String parameters) throws BigDawgCatalogException {
        this.username = usr;
        this.password = pw;
        this.domain = domain;
        this.port = port;
        this.endpoint = endpoint;
        if (parameters != null && parameters.length() > 0) {
            parseConnectionParameters(parameters);
        }
    }

    private void parseConnectionParameters(String paramaters) throws BigDawgCatalogException {
        List<NameValuePair> list = URLEncodedUtils.parse(paramaters, Charset.defaultCharset());
        this.connectionParameters = new HashMap<String,String>();
        for(NameValuePair pair: list) {
            if (this.connectionParameters.containsKey(pair.getName())) {
                throw new BigDawgCatalogException("Connection parameters for Api should not contain multiple key-values of the same name");
            }
            this.connectionParameters.put(pair.getName(), pair.getValue());
        }
    }

    @Override
    public String getUrl() {
        // TODO Auto-generated method stub
        try {
            Tuple.Tuple2<String, String> schemeAndPort = this.getSchemeAndPort();
            String prefix = "";
            if (this.connectionParameters.containsKey("prefix")) {
                prefix = this.connectionParameters.get("prefix");
            }
            return schemeAndPort.getT1() + "://" + this.domain + schemeAndPort.getT2() + prefix + "/" +  this.endpoint;
        }
        catch(ApiException e) {
            // @TODO - log this exception, make sure null return value is handled
            return null;
        }
    }

    private Tuple.Tuple2<String, String> getSchemeAndPort() throws ApiException {
        String scheme = null;
        if (this.port.equals("443") || this.port.equals("8443") || this.port.equals("8444")) {
            scheme = "https";
            if (this.port.equals("443")) {
                return new Tuple.Tuple2<String, String>(scheme, "");
            }
        }
        if (this.port.equals("80") || this.port.equals("8080") || this.port.equals("8888") || this.port.equals("8000")) {
            scheme = "http";
            if (this.port.equals("80")) {
                return new Tuple.Tuple2<String, String>(scheme, "");
            }
        }
        if (scheme != null) {
            return new Tuple.Tuple2<String, String>(scheme, ":" + this.port);
        }
        throw new ApiException("Can't determine scheme from port: " + this.port);
    }

    @Override
    public String getHost() {
        return domain;
    }

    @Override
    public String getPort() {
        return port;
    }

    @Override
    public String getUser() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDatabase() {
        return endpoint;
    }

    @Override
    public Collection<String> getCleanupQuery(Collection<String> objects) {
        return null;
    }

    @Override
    public long[] computeHistogram(String object, String attribute,
                                   double start, double end, int numBuckets)
            throws LocalQueryExecutionException {
        return null;
    }

    @Override
    public Pair<Number, Number> getMinMax(String object, String attribute)
            throws LocalQueryExecutionException, ParseException {
        return null;
    }

    @Override
    public ExecutorEngine getLocalQueryExecutor()
            throws LocalQueryExecutorLookupException {
        // @TODO Need to return this.
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ApiConnectionInfo [domain=" + domain
                + ", port=" + port + ", domain="
                + domain + ", username=" + username
                + ", password=" + "Sorry, I cannot show it" + "]";
    }

}

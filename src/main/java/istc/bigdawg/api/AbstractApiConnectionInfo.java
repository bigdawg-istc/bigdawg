package istc.bigdawg.api;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
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
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

abstract public class AbstractApiConnectionInfo implements ConnectionInfo {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    protected String host;
    protected int port;
    protected String scheme;
    protected String user;
    protected String password;
    protected String database;

    private AbstractApiConnectionInfo(String host, String port, String database, String user, String password) throws BigDawgCatalogException {
        this.host = host;
        this.port = Integer.parseInt(port);
        this.database = database;
        this.password = password;
        this.user = user;
    }

    public AbstractApiConnectionInfo(String host, String port, String database, String user, String password, String scheme) throws BigDawgCatalogException {
        this(host, port, database, user, password);
        this.scheme = scheme;
    }

    public static Map<String, String> parseConnectionParameters(String connectionParametersStr, String type) throws BigDawgCatalogException {
        assert(connectionParametersStr != null);

        Map<String, String> connectionParameters = new HashMap<String,String>();
        final String expectedPrefix = type + ":";
        if (!connectionParametersStr.startsWith(expectedPrefix)) {
            throw new BigDawgCatalogException("Expected connection parameters string to start with " + expectedPrefix);
        }

        final String connectionParametersStrPared = connectionParametersStr.substring(expectedPrefix.length());
        if (connectionParametersStrPared.length() == 0) {
            return connectionParameters;
        }

        String[] parametersList = connectionParametersStr.split(",");
        for(String parameter: parametersList) {
            String[] pair = parameter.split("=");
            if (pair.length != 2) {
                throw new BigDawgCatalogException("ApiConnectionParameters should be in the form of 'key=urlencoded(value)', instead found a pair of length " + pair.length + " with parameter \"" + parameter + "\" - which came from connection string: " + connectionParametersStr);
            }
            try {
                String value = URLDecoder.decode(pair[1], "UTF-8");
                String key = pair[0];
                if (key.length() == 0) {
                    throw new BigDawgCatalogException("key has no length - could not decode value \"" + pair[1] + "\" from pair: \"" + parameter + "\" in connection string: " + connectionParametersStr);
                }
                else if (value.length() == 0) {
                    throw new BigDawgCatalogException("Value has no length - could not decode value \"" + pair[1] + "\" from pair: \"" + parameter + "\" in connection string: " + connectionParametersStr);
                }
                if (connectionParameters.containsKey(key)) {
                    throw new BigDawgCatalogException("Duplicate key - could not decode value \"" + pair[1] + "\" from pair: \"" + parameter + "\" in connection string: " + connectionParametersStr);
                }
                connectionParameters.put(key, value);
            }
            catch (UnsupportedEncodingException e) {
                throw new BigDawgCatalogException("UnsupportedEncoding - " + e.getMessage() + " - Could not decode value \"" + pair[1] + "\" from pair: \"" + parameter + "\" in connection string: " + connectionParametersStr);
            }
        }
        return connectionParameters;
    }

    @Override
    public String getUrl() {
        return this.getUrlPrefix();
    }

    private String getUrlPrefix() {
        try {
            Tuple.Tuple2<String, String> schemeAndPort = this.getSchemeAndPort();
            return schemeAndPort.getT1() + "://" + this.host + schemeAndPort.getT2();
        }
        catch(ApiException e) {
            // @TODO - log this exception, make sure null return value is handled
            return null;
        }
    }

    private Tuple.Tuple2<String, String> getSchemeAndPort() throws ApiException {
        if (this.scheme == null) {
            if (this.port == 443 || this.port == 8443 || this.port == 8444) {
                this.scheme = "https";
            }
            else if (this.port == 80 || this.port == 8080 || this.port == 8888 || this.port == 8000) {
                this.scheme = "http";
            }
            throw new ApiException("Can't auto-determine scheme from port: " + this.port);
        }

        String portStr;
        if (this.port == 80 && this.scheme.equals("http")) {
            portStr = "";
        }
        else if (this.port == 443 && this.scheme.equals("https")) {
            portStr = "";
        }
        else {
            portStr = ":" + String.valueOf(this.port);
        }
        return new Tuple.Tuple2<String, String>(scheme, portStr);
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getPort() {
        return String.valueOf(port);
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDatabase() {
        return database;
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
        return "ApiConnectionInfo [host=" + host
                + ", port=" + String.valueOf(port) + ", scheme="
                + scheme + ", user=" + user
                + ", password=" + "Sorry, I cannot show it" + "]";
    }

}

package istc.bigdawg.rest;

import istc.bigdawg.api.*;
import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.RESTQueryResult;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

public class RESTConnectionInfo extends AbstractApiConnectionInfo {
    private final static int ReadTimeout = 120000; // ms
    private final static int ConnectTimeout = 60000; // ms

    private int authReadTimeout;
    private int authConnectTimeout;
    private int readTimeout;
    private int connectTimeout;
    private String resultKey; // Should be set to null if not existing
    private URL url;
    private HttpMethod method;
    private AuthenticationType authenticationType;
    private Map<String, String> connectionProperties;
    private static Logger log = Logger
            .getLogger(RESTConnectionInfo.class.getName());

    // Need this to hold results as RESTHandler can get reinstantiated during the lifecycle of a request.
    transient private Map<String, RESTQueryResult> restQueryResults = new HashMap<>();

    private RESTConnectionInfo(String host, String port,
                             String endpoint, String user, String password, Map<String, String> connectionProperties, URL url, String fields) throws BigDawgCatalogException {
        super(host, port, endpoint, user, connectionProperties.getOrDefault("password", null), password, connectionProperties.getOrDefault("scheme", url.getProtocol()));
        this.connectionProperties = connectionProperties;
        this.url = url;
        this.parseAuthenticationType();
        this.parseMethod();
        this.parseTimeouts();
        this.parseFields(fields);
        this.populateEncodingMode();
    }

    private RESTConnectionInfo(String endpoint, Map<String, String> connectionProperties, URL url, String user, String password, String fields) throws BigDawgCatalogException {
        this(url.getHost(),
                String.valueOf(url.getPort()),
                endpoint,
                user != null && user.length() > 0 ? user : connectionProperties.getOrDefault("userid", connectionProperties.getOrDefault("consumer_key", null)),
                password,
                connectionProperties,
                url,
                fields);
    }

    public RESTConnectionInfo(String endpoint, String connectionPropertiesStr, String urlStr, String user, String password, String fields) throws BigDawgCatalogException, MalformedURLException {
        this(endpoint, AbstractApiConnectionInfo.parseConnectionProperties(connectionPropertiesStr, "REST"), new URL(urlStr), user, password, fields);
    }

    private void populateEncodingMode() {
        if (this.passwordProperties.containsKey("post_encoding")) {
            this.contentType = this.passwordProperties.get("post_encoding");
        }
    }

    private void parseFields(String fields) throws BigDawgCatalogException {
        if (fields == null || fields.length() == 0) {
            return;
        }
        String[] fieldList = fields.split(",");
        if (fieldList.length > 1) {
            throw new BigDawgCatalogException("Fields should only contain one key - if the key contains a comma, it must be URLEncoded");
        }
        try {
            resultKey = URLDecoder.decode(fields, "UTF-8");
        }
        catch (Exception e) {
            throw new BigDawgCatalogException("Could not decode fields: " + e.toString());
        }
    }

    private void parseTimeouts() {
        String connectTimeoutStr = this.connectionProperties.getOrDefault("connect_timeout", null);
        connectTimeout = connectTimeoutStr != null ? Integer.valueOf(connectTimeoutStr) : ConnectTimeout;
        String readTimeoutStr = this.connectionProperties.getOrDefault("read_timeout", null);
        readTimeout = readTimeoutStr != null ? Integer.valueOf(readTimeoutStr) : ReadTimeout;

        String authConnectTimeoutStr = this.connectionProperties.getOrDefault("auth_connect_timeout", connectTimeoutStr);
        authConnectTimeout = authConnectTimeoutStr != null ? Integer.valueOf(authConnectTimeoutStr) : ConnectTimeout;

        String authReadTimeoutStr = this.connectionProperties.getOrDefault("auth_read_timeout", readTimeoutStr);
        authReadTimeout = authReadTimeoutStr != null ? Integer.valueOf(authReadTimeoutStr) : ReadTimeout;
    }

    private void parseMethod() throws BigDawgCatalogException {
        if (this.passwordProperties.containsKey("method")) {
            String methodStr = this.passwordProperties.get("method");
            this.method = HttpMethod.parseMethod(methodStr);
            if (this.method == null) {
                throw new BigDawgCatalogException("Unsupported / unknown method: " + methodStr);
            }
            return;
        }
        this.method = HttpMethod.GET;
    }

    private void parseAuthenticationType() throws BigDawgCatalogException {
        if (!this.connectionProperties.containsKey("auth_type")) {
            this.authenticationType = AuthenticationType.NONE;
            return;
        }
        String authenticationType = this.connectionProperties.get("auth_type");

        switch(authenticationType) {
            case "basic":
                this.authenticationType = AuthenticationType.BASIC;
                break;
            case "token":
                // @TODO do we need to differentiate between tokens that are in the URL and tokens that are in the headers
                this.authenticationType = AuthenticationType.TOKEN;
                if (!this.connectionProperties.containsKey("token")) {
                    throw new BigDawgCatalogException("Expecting token to be set in connection parameters");
                }
                String key = "token";
                if (this.connectionProperties.containsKey("token_key")) {
                    key = this.connectionProperties.get("token_key");
                }
                if (this.extraQueryParameters.containsKey("key")) {
                    throw new BigDawgCatalogException("Token key " + key + " is already in query_params");
                }
                // @TODO what happens if this is supposed to be in POST params instead of query params?
                this.extraQueryParameters.put(key, this.connectionProperties.get("token"));
                break;
            case "bearer":
                this.authenticationType = AuthenticationType.BEARER;
                if (!this.connectionProperties.containsKey("token") && !this.connectionProperties.containsKey("bearer_token")) {
                    throw new BigDawgCatalogException("Expecting token to be set in connection parameters");
                }
                break;
            case "oauth1":
                this.authenticationType = AuthenticationType.OAUTH1;
                OAuth1.verifyConnectionProperties(this.connectionProperties);
                break;
            case "oauth2":
                this.authenticationType = AuthenticationType.OAUTH2;
                OAuth2.verifyConnectionProperties(this.connectionProperties, this.getUser(), this.connectionProperties.getOrDefault("consumer_secret", null));
                break;
            default:
                throw new BigDawgCatalogException("Unknown authentication type: " + authenticationType);
        }
    }

    public HttpMethod getMethod() {
        return this.method;
    }

    public String getPostEncoding() {
        return this.passwordProperties.getOrDefault("post_encoding", "application/x-www-form-urlencoded");
    }

    Map<String, String> getHeaders(String queryString) throws ApiException {
        Map<String, String> headers = new HashMap<>();
        switch (this.authenticationType) {
            case OAUTH1:
                try {
                    Map<String, String> queryParameters = URLUtil.parseQueryString(queryString);
                    if (this.extraQueryParameters != null) {
                        for (String key: this.extraQueryParameters.keySet()) {
                            if (!queryParameters.containsKey(key)) {
                                queryParameters.put(key, this.extraQueryParameters.get(key));
                            }
                        }
                    }
                    headers.put("Authorization", OAuth1.getOAuth1Header(this.method, this.getUrl(), this.connectionProperties, queryParameters, null, null));
                    headers.put("Accept", "*/*");
                }
                catch (Exception e) {
                    throw new ApiException(e.toString());
                }
                break;
            case BEARER:
                headers.put("Authorization", "Bearer " + this.connectionProperties.getOrDefault("bearer_token", this.connectionProperties.get("token")));
                break;
            case BASIC:
                String authStr = this.connectionProperties.getOrDefault("username", "") + ":" + this.connectionProperties.getOrDefault("password", "");
                headers.put("Authorization", "Basic " + (new String(Base64.getEncoder().encode(authStr.getBytes()))));
                break;
            case OAUTH2:
                String authUrl = connectionProperties.getOrDefault("auth_url", "/");
                if (!authUrl.startsWith("http")) {
                    if (authUrl.startsWith("/")) {
                        authUrl = this.getUrlPrefix() + authUrl;
                    }
                    else {
                        authUrl = this.getUrlPrefix() + "/" + authUrl;
                    }
                }
                String password = this.connectionProperties.getOrDefault("consumer_secret", null);
                if (password == null) {
                    password = getPassword();
                }
                OAuth2.setOAuth2Headers(headers, connectionProperties, getUser(), password, authUrl, authConnectTimeout, authReadTimeout);

                break;
        }
        return headers;
    }

    String getFinalQueryParameters(String queryString) throws QueryParsingException {
        if (this.extraQueryParameters == null || this.extraQueryParameters.size() == 0) {
            return queryString;
        }
        if (queryString == null) {
            return URLUtil.encodeParameters(this.extraQueryParameters);
        }

        Map<String, String> queryParameters = URLUtil.parseQueryString(queryString);
        for (String key: this.extraQueryParameters.keySet()) {
            if (!queryParameters.containsKey(key)) {
                queryParameters.put(key, this.extraQueryParameters.get(key));
            }
        }
        return URLUtil.encodeParameters(queryParameters);
    }

    /**
     * Returns the actual URL
     * @return URL
     */
    public URL getURL() {
        // Note: it's a bit hacky to name it the same as the other method except having a different
        // case, but Java doesn't support method overloading unless the parameters are different.
        return this.url;
    }

    @Override
    public String getUrl() {
        return this.url.toString();
    }

    int getConnectTimeout() {
        return connectTimeout;
    }

    int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Should return null if the key is blank (empty string)
     * @return null|String
     */
    String getResultKey() {
        return resultKey;
    }

    public void setResultKey(String resultKey) {
        this.resultKey = resultKey;
    }

    @Override
    public ExecutorEngine getLocalQueryExecutor()
            throws LocalQueryExecutorLookupException {
        try {
            return new RESTHandler(this);
        } catch (Exception e) {
            e.printStackTrace();
            throw new LocalQueryExecutorLookupException("Cannot construct "
                    + RESTHandler.class.getName());
        }
    }

    void putRESTQueryResult(String key, RESTQueryResult restQueryResult) {
        this.restQueryResults.put(key, restQueryResult);
    }

    RESTQueryResult getRESTQueryResult(String key) {
        return this.restQueryResults.getOrDefault(key, null);
    }
}

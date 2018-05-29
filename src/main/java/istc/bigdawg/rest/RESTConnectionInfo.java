package istc.bigdawg.rest;

import istc.bigdawg.api.*;
import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class RESTConnectionInfo extends AbstractApiConnectionInfo {
    private String prefix;
    private HttpMethod method;
    private AuthenticationType authenticationType;
    private Map<String, String> connectionParameters;
    private Map<String, String> extraQueryParameters;
    private static Logger log = Logger
            .getLogger(RESTConnectionInfo.class.getName());

    private RESTConnectionInfo(String host, String port,
                             String endpoint, String user, String password, Map<String, String> connectionParameters) throws BigDawgCatalogException {
        super(host, port, endpoint, user, password, connectionParameters.getOrDefault("scheme", null));
        this.connectionParameters = connectionParameters;
        this.parseExtraQueryParameters(); // This should happen before authentication type
        this.parseAuthenticationType();
        this.parsePrefix();
        this.parseMethod();
    }
    public RESTConnectionInfo(String host, String port,
                             String endpoint, String user, String password, String connectionParametersStr) throws BigDawgCatalogException {
        this(host, port, endpoint, user, password, AbstractApiConnectionInfo.parseConnectionParameters(connectionParametersStr, "REST"));
    }

    private void parseExtraQueryParameters() throws BigDawgCatalogException {
        this.extraQueryParameters = new HashMap<String, String>();
        if (!this.connectionParameters.containsKey("query_params")) {
            return;
        }
        String extraQueryParameters = this.connectionParameters.get("query_params");
        String[] pairs = extraQueryParameters.split("&");
        try {
            for (String pair : pairs) {
                int idx = pair.indexOf('=');
                if (idx < 0) {
                    throw new BigDawgCatalogException("Could not find '=' in extra query param pair: " + pair);
                }
                String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                String value = "";
                if (idx < pair.length() - 1) {
                    value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");
                }
                if (this.extraQueryParameters.containsKey(key)) {
                    throw new BigDawgCatalogException("Redundant extra query parameter '" + key + "'");
                }
                this.extraQueryParameters.put(key, value);
            }
        }
        catch (Exception e) {
            throw new BigDawgCatalogException(e.getMessage());
        }
    }

    private void parseMethod() throws BigDawgCatalogException {
        if (this.connectionParameters.containsKey("method")) {
            String methodStr = this.connectionParameters.get("method");
            this.method = HttpMethod.parseMethod(methodStr);
            if (this.method == null) {
                throw new BigDawgCatalogException("Unsupported / unknown method: " + methodStr);
            }
        }
        this.method = HttpMethod.GET;
    }

    private void parsePrefix() {
        if (this.connectionParameters.containsKey("prefix")) {
            this.prefix = this.connectionParameters.get("prefix");
            if (!this.prefix.startsWith("/")) {
                this.prefix = "/" + this.prefix;
            }
        }
        else {
            this.prefix = "/";
        }
    }

    private void parseAuthenticationType() throws BigDawgCatalogException {
        if (!this.connectionParameters.containsKey("auth_type")) {
            this.authenticationType = AuthenticationType.NONE;
            return;
        }
        String authenticationType = this.connectionParameters.get("auth_type");

        switch(authenticationType) {
            case "basic":
                this.authenticationType = AuthenticationType.BASIC;
                break;
            case "token":
                // @TODO do we need to differentiate between tokens that are in the URL and tokens that are in the headers
                this.authenticationType = AuthenticationType.TOKEN;
                if (!this.connectionParameters.containsKey("token")) {
                    throw new BigDawgCatalogException("Expecting token to be set in connection parameters");
                }
                String key = "token";
                if (this.connectionParameters.containsKey("token_key")) {
                    key = this.connectionParameters.get("token_key");
                }
                if (this.extraQueryParameters.containsKey("key")) {
                    throw new BigDawgCatalogException("Token key " + key + " is already in query_params");
                }
                // @TODO what happens if this is supposed to be in POST params instead of query params?
                this.extraQueryParameters.put(key, this.connectionParameters.get("token"));
                break;
            case "bearer":
                this.authenticationType = AuthenticationType.BEARER;
                if (!this.connectionParameters.containsKey("token")) {
                    throw new BigDawgCatalogException("Expecting token to be set in connection parameters");
                }
                break;
            case "oauth1":
                this.authenticationType = AuthenticationType.OAUTH1;
                OAuth1.verifyConnectionParameters(this.connectionParameters);
                break;
            case "oauth2":
                this.authenticationType = AuthenticationType.OAUTH2;
                OAuth2.verifyConnectionParameters(this.connectionParameters, this.getUser(), this.getPassword());
                break;
            default:
                throw new BigDawgCatalogException("Unknown authentication type: " + authenticationType);
        }
    }

    public HttpMethod getMethod() {
        return this.method;
    }

    public Map<String, String> getHeaders() throws ApiException {
        Map<String, String> headers = new HashMap<>();
        switch (this.authenticationType) {
            case OAUTH1:
                headers.put("OAuth", OAuth1.getOAuth1Header(this.method, this.getEndpointUrl(), this.connectionParameters));
                break;
            case BEARER:
                headers.put("Authorization", "Bearer " + this.connectionParameters.get("token"));
                break;
            case OAUTH2:
                OAuth2.setOAuth2Headers(headers, connectionParameters, getUser(), getPassword());
                break;
        }
        return headers;
    }

    private String getEndpointUrl() {
        String baseUrl = super.getUrl();
        String prefixUrl = baseUrl + this.prefix;
        return prefixUrl.endsWith("/") ? prefixUrl + this.getEndpoint() : prefixUrl + "/" + this.getEndpoint();
    }

    @Override
    public String getUrl() {
        String endpointUrl = this.getEndpointUrl();
        try {
            return URLUtil.appendQueryParameters(endpointUrl, this.extraQueryParameters);
        }
        catch (Exception e) {
            RESTConnectionInfo.log.error("Error trying to get url", e);
            return null;
        }
    }

    public AuthenticationType getAuthenticationType() {
        return this.authenticationType;
    }

    public String getEndpoint() {
        return this.getDatabase();
    }
}

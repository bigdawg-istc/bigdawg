package istc.bigdawg.api;

import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.interfaces.Response;
import org.apache.commons.httpclient.HttpURL;
import org.apache.tools.ant.taskdefs.condition.Http;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.text.Bidi;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

class OAuth2 {
    private final static int ConnectTimeout = 20000; // ms
    private final static int ReadTimeout = 20000; // ms
    private final static String UserAgent = "bigdawg/1";

    static enum AuthenticationType {
        BASIC_BEARER
    }

    static enum ResponseType {
        JSON
    }

    private static AuthenticationType getAuthenticationTypeFromString(String authenticationTypeStr) throws BigDawgCatalogException {
        if (authenticationTypeStr == null) {
            return AuthenticationType.BASIC_BEARER;
        }

        switch (authenticationTypeStr) {
            case "basic_bearer":
            case "Basic_Bearer":
            case "BASIC_BEARER":
                return AuthenticationType.BASIC_BEARER;
            default:
                throw new BigDawgCatalogException("Unknown / Unsupported OAuth2 authentication type: " + authenticationTypeStr);
        }
    }

    static void setOAuth2Headers(Map<String, String> headers, Map<String, String> connectionParameters, String user, String password) {
        try {
            AuthenticationType oauth2AuthType = getAuthenticationTypeFromString(connectionParameters.get("oauth2_auth_type"));
            if (oauth2AuthType == null) {
                oauth2AuthType = AuthenticationType.BASIC_BEARER;
            }
            switch(oauth2AuthType) {
                case BASIC_BEARER:
                    String url = connectionParameters.get("auth_url");
                    String credentials = user + ":" + password;
                    String credentialsBase64 = new String(Base64.getEncoder().encode(credentials.getBytes()));
                    HttpMethod method = HttpMethod.GET;
                    if (connectionParameters.containsKey("auth_method")) {
                        method = HttpMethod.parseMethod(connectionParameters.get("auth_method"));
                        assert(method != null);
                    }
                    String postData = null;
                    if (method == HttpMethod.POST) {
                        // See if there's POST data
                        if (connectionParameters.containsKey("auth_post_data")) {
                            postData = connectionParameters.get("auth_post_data");
                            postData = URLDecoder.decode(postData, "UTF-8");
                        }
                    }
                    ResponseType responseType = ResponseType.JSON;
                    String responseKey = "access_token";
                    if (connectionParameters.containsKey("auth_response_key")) {
                        responseKey = connectionParameters.get("auth_response_key");
                    }
                    Map<String, String> validationPairs = parseAuthResponseValidate(connectionParameters.getOrDefault("auth_response_validate", null));
                    String token = getBearerTokenFromURL(url, method, responseType, credentialsBase64, postData, responseKey, validationPairs);
                    headers.put("Authorization", "Bearer " + token);
                    break;
            }
        }
        catch (Exception e) {
            // @TODO log errors
        }
    }

    private static String getBearerTokenFromURL(String urlStr, HttpMethod method, ResponseType responseType, String credentials, String postData, String responseKey, Map<String, String> validationPairs) throws IOException, ParseException {
        URL url = new URL(urlStr);
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.setConnectTimeout(ConnectTimeout);
        urlConnection.setReadTimeout(ReadTimeout);
        urlConnection.setUseCaches(false);
        urlConnection.setRequestMethod(method.name());
        urlConnection.setRequestProperty("User-Agent", UserAgent);
        urlConnection.setRequestProperty("Authorization", "Basic " + credentials);
        urlConnection.setDoInput(true);
        InputStream inputStream = urlConnection.getInputStream();
        if (postData != null && method == HttpMethod.POST) {
            urlConnection.setDoOutput(true);
            urlConnection.setRequestProperty("Content-Length", String.valueOf(postData.length()));
            urlConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); // @TODO - abstract this to a key
            OutputStream os = urlConnection.getOutputStream();
            DataOutputStream dataOutputStream= new DataOutputStream(new BufferedOutputStream(os));
            dataOutputStream.writeBytes(postData);
            dataOutputStream.close();
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder responseBuilder = new StringBuilder();
        char[] buffer = new char[4096];
        while (bufferedReader.read(buffer, 0,4096) == 4096) {
            responseBuilder.append(buffer);
        }
        responseBuilder.append(buffer);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(responseBuilder.toString());
        for(String key: validationPairs.keySet()) {
            String result = (String) jsonObject.get(key);
            String expectedResult = validationPairs.get(key);
            if (!expectedResult.equals(result)) {
                throw new ParseException(-1 , "Result not as expected - expected '" + key + "=" + expectedResult + "' instead got result: '" + (result == null ? "(null)" : result));
            }
        }
        String value = (String) jsonObject.get(responseKey);
        if (value == null) {
            throw new ParseException(-1, "Could not find '" + responseKey + "' in result: " + responseBuilder.toString());
        }

        return value;
    }


    static void verifyConnectionParameters(Map<String, String> connectionParameters, String user, String password) throws BigDawgCatalogException {
        if (!connectionParameters.containsKey("auth_url")) {
            throw new BigDawgCatalogException("For oauth2, need an auth_url");
        }
        AuthenticationType oauth2AuthType = getAuthenticationTypeFromString(connectionParameters.get("oauth2_auth_type"));
        if (oauth2AuthType == null) {
            oauth2AuthType = AuthenticationType.BASIC_BEARER;
        }

        switch(oauth2AuthType) {
            case BASIC_BEARER:
                if (user == null || user.length() == 0) {
                    throw new BigDawgCatalogException("For oauth2, need a user in the database");
                }
                if (password == null || password.length() == 0) {
                    throw new BigDawgCatalogException("For oauth2, need a user in the database");
                }
                break;
            default:
                throw new BigDawgCatalogException("Unknown / Unsupported OAuth2 authentication type: " + oauth2AuthType.name());
        }
        if (connectionParameters.containsKey("auth_method")) {
            String authMethodStr = connectionParameters.get("auth_method");
            HttpMethod authMethod = HttpMethod.parseMethod(authMethodStr);
            if (authMethod == null) {
                throw new BigDawgCatalogException("Unknown / Unsupported auth_method: " + authMethodStr);
            }
        }

        if (connectionParameters.containsKey("auth_response_type")) {
            if (!connectionParameters.get("auth_response_type").toUpperCase().equals("JSON")) {
                throw new BigDawgCatalogException("Unknown / Unsupported response type: " + connectionParameters.get("auth_response_type"));
            }
        }
        if (connectionParameters.containsKey("auth_response_validate")) {
            String responseValidationPairs = connectionParameters.get("auth_response_validate");
            parseAuthResponseValidate(responseValidationPairs);
        }
    }

    private static Map<String, String> parseAuthResponseValidate(String responseValidationPairs) throws BigDawgCatalogException {
        Map<String, String> resultPairs = new HashMap<String, String>();
        if (responseValidationPairs == null) {
            return resultPairs;
        }

        try {
            responseValidationPairs = URLDecoder.decode(responseValidationPairs, "UTF-8");
            String [] pairs = responseValidationPairs.split("&");
            for(String pair: pairs) {
                String [] keyValue = pair.split("=");
                if (keyValue.length != 2) {
                    throw new BigDawgCatalogException("Expected a key=value pair from auth_response_validate: " + pair);
                }
                String key = URLDecoder.decode(keyValue[0], "UTF-8");
                String value = URLDecoder.decode(keyValue[1], "UTF-8");
                if (resultPairs.containsKey(key)) {
                    throw new BigDawgCatalogException("Duplicate key found: " + key);
                }
                resultPairs.put(key, value);
            }
        }
        catch (Exception e) {
            throw new BigDawgCatalogException("Exception while decoding responseValidationPairs '" + responseValidationPairs + "': " + e.getMessage());
        }
        return resultPairs;
    }

}

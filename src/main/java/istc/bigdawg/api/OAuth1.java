package istc.bigdawg.api;

import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.BigDawgCatalogException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.*;

class OAuth1 {
    static String createSignature(String method, String url, Map<String, String> parameters, String consumerSecret, String tokenSecret) throws ApiException {
        SortedSet<String> keys = new TreeSet<String>(parameters.keySet());

        StringJoiner joiner = new StringJoiner("&");
        for (String key: keys) {
            joiner.add(URLUtil.percentEncode(key) + "=" + URLUtil.percentEncode(parameters.get(key)));
        }
        String finalParameters = joiner.toString();

        StringJoiner finalJoin = new StringJoiner("&");
        finalJoin.add(method.toUpperCase());
        finalJoin.add(URLUtil.percentEncode(url));
        finalJoin.add(URLUtil.percentEncode(finalParameters));

        final String signatureBaseString = finalJoin.toString();

        final String signatureMethod = parameters.get("oauth_signature_method");
        switch(signatureMethod) {
            case "HMAC-SHA1":
                try {
                    StringJoiner signingKeyJoiner = new StringJoiner("&");
                    signingKeyJoiner.add(URLUtil.percentEncode(consumerSecret));
                    signingKeyJoiner.add(URLUtil.percentEncode(tokenSecret));
                    final String signingKeyStr = signingKeyJoiner.toString();
                    SecretKeySpec key = new SecretKeySpec(signingKeyStr.getBytes(), "HmacSHA1");

                    Mac mac = Mac.getInstance("HmacSHA1");
                    mac.init(key);

                    byte[] result = mac.doFinal(signatureBaseString.getBytes());
                    return new String(Base64.getEncoder().encode(result));
                }
                catch (Exception e) {
                    throw new ApiException("Error trying to sign: " + e.getMessage());
                }
            default:
                throw new ApiException("Unsupported signature method: " + signatureMethod);
        }
    }

    private String getOAuth1Header(HttpMethod method, String endpointUrl, Map<String, String> connectionParameters) throws ApiException {
        String timestamp = String.valueOf(Instant.now().getEpochSecond());
        StringBuilder nonceSB = new StringBuilder();
        try {
            byte[] nonceBytes = new byte[32];
            SecureRandom.getInstanceStrong().nextBytes(nonceBytes);
            byte[] encoded = Base64.getEncoder().encode(nonceBytes);

            for(byte b: encoded) {
                switch((char) b) {
                    case '=':
                    case '+':
                    case '/':
                        break;
                    default:
                        nonceSB.append(b);
                }
            }
        }
        catch (Exception e) {
            // @Todo - how to die gracefully here?
        }
        String nonce = nonceSB.toString();
        String consumerKey = connectionParameters.get("oauth_consumer_key");
        String oauthVersion = connectionParameters.get("oauth_version");
        String oauthToken = connectionParameters.get("oauth_token");
        String oauthSignatureMethod = connectionParameters.get("oauth_signature_method");
        Map<String, String> oauthParameters = new HashMap<String, String>();
        oauthParameters.put("oauth_timestamp", timestamp);
        oauthParameters.put("oauth_signature_method", oauthSignatureMethod);
        oauthParameters.put("oauth_nonce", nonce.toString());
        oauthParameters.put("oauth_consumer_key", consumerKey);
        oauthParameters.put("oauth_token", oauthToken);
        if (oauthVersion != null && oauthVersion.length() > 0) {
            oauthParameters.put("oauth_version", oauthVersion);
        }
        oauthParameters.put("oauth_signature_method", oauthSignatureMethod);
        String oauthSignature = OAuth1.createSignature(method.name(), endpointUrl, oauthParameters, connectionParameters.get("oauth_consumer_secret"), connectionParameters.get("oauth_token_secret"));
        StringBuilder sb = new StringBuilder();
        sb.append("oauth_consumer_key=");
        sb.append(connectionParameters.get("oauth_consumer_key"));
        sb.append(", ");
        sb.append("oauth_nonce=");
        sb.append(nonce);
        sb.append(", ");
        sb.append("oauth_signature=");
        sb.append(oauthSignature);
        sb.append(", ");
        sb.append("oauth_signature_method=");
        sb.append(oauthSignatureMethod);
        sb.append(", ");
        sb.append("oauth_timestamp=");
        sb.append(timestamp);
        sb.append(", ");
        sb.append("oauth_token=");
        sb.append(oauthToken);
        if (oauthVersion != null && oauthVersion.length() > 0) {
            sb.append(", ");
            sb.append("oauth_version=");
            sb.append(oauthVersion);
        }

        return sb.toString();
    }



    static void verifyConnectionParameters(Map<String, String> connectionParameters) throws BigDawgCatalogException {
        if (!connectionParameters.containsKey("oauth_consumer_key")) {
            throw new BigDawgCatalogException("Expecting oauth_consumer_key to be set in connection parameters");
        }
        if (!connectionParameters.containsKey("oauth_signature_method")) {
            throw new BigDawgCatalogException("Expecting oauth_signature_method to be set in connection parameters");
        }
        if (!connectionParameters.containsKey("oauth_token")) {
            throw new BigDawgCatalogException("Expecting oauth_token to be set in connection parameters");
        }
        if (!connectionParameters.containsKey("oauth_token_secret")) {
            throw new BigDawgCatalogException("Expecting oauth_token_secret to be set in connection parameters");
        }
        if (!connectionParameters.containsKey("oauth_consumer_secret")) {
            throw new BigDawgCatalogException("Expecting oauth_consumer_secret to be set in connection parameters");
        }
    }
}

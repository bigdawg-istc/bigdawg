package istc.bigdawg.rest;

import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import org.apache.log4j.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.*;

class OAuth1 {
    private static Logger log = Logger
            .getLogger(OAuth1.class.getName());

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
                    SecretKeySpec key = new SecretKeySpec(signingKeyStr.getBytes(StandardCharsets.UTF_8), "HmacSHA1");

                    Mac mac = Mac.getInstance("HmacSHA1");
                    mac.init(key);

                    byte[] result = mac.doFinal(signatureBaseString.getBytes(StandardCharsets.UTF_8));
                    return new String(Base64.getEncoder().encode(result));
                }
                catch (Exception e) {
                    throw new ApiException("Error trying to sign: " + e.getMessage());
                }
            default:
                throw new ApiException("Unsupported signature method: " + signatureMethod);
        }
    }

    static String getOAuth1Header(HttpMethod method, String endpointUrl, Map<String, String> connectionParameters, Map<String, String> queryParameters, String nonceTmp, String timestampTmp) throws ApiException {
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
                        nonceSB.append((char) b);
                }
            }
        }
        catch (Exception e) {
            OAuth1.log.error("Exception trying to create nonce", e);
            return null;
        }
        String nonce = nonceTmp != null? nonceTmp: nonceSB.toString();
        String consumerKey = connectionParameters.getOrDefault("oauth_consumer_key", connectionParameters.get("consumer_key"));
        String oauthVersion = connectionParameters.get("oauth_version");
        String oauthToken = connectionParameters.getOrDefault("oauth_token", connectionParameters.get("access_token"));
        String oauthSignatureMethod = connectionParameters.getOrDefault("oauth_signature_method", connectionParameters.get("signature_method"));
        Map<String, String> oauthParameters = new HashMap<String, String>();
        for (String key: queryParameters.keySet()) {
            oauthParameters.put(key, queryParameters.get(key));
        }
        oauthParameters.put("oauth_timestamp", timestampTmp != null ? timestampTmp : timestamp);
        oauthParameters.put("oauth_signature_method", oauthSignatureMethod);
        oauthParameters.put("oauth_nonce", nonce);
        oauthParameters.put("oauth_consumer_key", consumerKey);
        oauthParameters.put("oauth_token", oauthToken);
        if (oauthVersion != null && oauthVersion.length() > 0) {
            oauthParameters.put("oauth_version", oauthVersion);
        }
        oauthParameters.put("oauth_signature_method", oauthSignatureMethod);
        String consumerSecret = connectionParameters.getOrDefault("oauth_consumer_secret", connectionParameters.get("consumer_secret"));
        String tokenSecret = connectionParameters.getOrDefault("oauth_token_secret", connectionParameters.get("access_token_secret"));
        String oauthSignature = OAuth1.createSignature(method.name(), endpointUrl, oauthParameters, consumerSecret, tokenSecret);
        StringBuilder sb = new StringBuilder();
        sb.append("OAuth ");
        sb.append("oauth_consumer_key=");
        sb.append('"');
        sb.append(consumerKey);
        sb.append('"');
        sb.append(", ");
        sb.append("oauth_nonce=");
        sb.append('"');
        sb.append(nonce);
        sb.append('"');
        sb.append(", ");
        sb.append("oauth_signature=");
        sb.append('"');
        try {
            sb.append(URLEncoder.encode(oauthSignature, "UTF-8"));
        }
        catch (UnsupportedEncodingException e) {
            throw new ApiException(e.toString());
        }
        sb.append('"');
        sb.append(", ");
        sb.append("oauth_signature_method=");
        sb.append('"');
        sb.append(oauthSignatureMethod);
        sb.append('"');
        sb.append(", ");
        sb.append("oauth_timestamp=");
        sb.append('"');
        sb.append(timestampTmp == null ? timestamp : timestampTmp);
        sb.append('"');
        sb.append(", ");
        sb.append("oauth_token=");
        sb.append('"');
        sb.append(oauthToken);
        sb.append('"');
        if (oauthVersion != null && oauthVersion.length() > 0) {
            sb.append(", ");
            sb.append("oauth_version=");
            sb.append('"');
            sb.append(oauthVersion);
            sb.append('"');
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        String consumerKey = "xvz1evFS4wEEPTGEFPHBog";
        String nonce = "kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg";
        String signatureMethod = "HMAC-SHA1";
        String timestamp = "1318622958";
        String token = "370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb";
        String version = "1.0";
        String method = "POST";
        String status = "Hello Ladies + Gentlemen, a signed OAuth request!";
        Map<String, String> parameters = new HashMap<>();
        parameters.put("oauth_consumer_key", consumerKey);
        parameters.put("oauth_nonce", nonce);
        parameters.put("oauth_signature_method", signatureMethod);
        parameters.put("include_entities", "true");
        parameters.put("status", status);
        parameters.put("oauth_timestamp", timestamp);
        parameters.put("oauth_version", version);
        parameters.put("oauth_token", token);

        String consumerSecret = "kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw";
        String tokenSecret = "LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE";

        String url = "https://api.twitter.com/1.1/statuses/update.json";
        try {
            String signature = createSignature(method, url, parameters, consumerSecret, tokenSecret);
            System.out.println(signature);

            Map<String, String> connectionParameters = new HashMap<>();
            connectionParameters.put("oauth_version", "1.0");
            connectionParameters.put("access_token", token);
            connectionParameters.put("consumer_key", consumerKey);
            connectionParameters.put("signature_method", signatureMethod);
            connectionParameters.put("consumer_secret", consumerSecret);
            connectionParameters.put("access_token_secret", tokenSecret);
            Map<String, String> queryParameters = new HashMap<>();
            queryParameters.put("include_entities", "true");
            queryParameters.put("status", status);
            String result = getOAuth1Header(HttpMethod.POST, url, connectionParameters, queryParameters, nonce, timestamp);
            System.out.println(result);
        }
        catch (Exception e) {
            System.out.println(e.toString());
        }

    }

    static void verifyConnectionProperties(Map<String, String> connectionProperties) throws BigDawgCatalogException {
        if (!connectionProperties.containsKey("oauth_consumer_key") && !connectionProperties.containsKey("consumer_key")) {
            throw new BigDawgCatalogException("Expecting oauth_consumer_key or consumer_key to be set in connection parameters");
        }
        if (!connectionProperties.containsKey("oauth_signature_method") && !connectionProperties.containsKey("signature_method")) {
            throw new BigDawgCatalogException("Expecting oauth_signature_method or signature_method to be set in connection parameters");
        }
        if (!connectionProperties.containsKey("oauth_token") && !connectionProperties.containsKey("access_token")) {
            throw new BigDawgCatalogException("Expecting oauth_token or access_token to be set in connection parameters");
        }
        if (!connectionProperties.containsKey("oauth_token_secret") && !connectionProperties.containsKey("access_token_secret")) {
            throw new BigDawgCatalogException("Expecting oauth_token_secret or accces_token_secret to be set in connection parameters");
        }
        if (!connectionProperties.containsKey("oauth_consumer_secret") && !connectionProperties.containsKey("consumer_secret")) {
            throw new BigDawgCatalogException("Expecting oauth_consumer_secret or consumer_secret to be set in connection parameters");
        }
    }
}

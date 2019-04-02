package istc.bigdawg.rest;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class OAuth1Test {

    @Test
    public void testBasicSignature() {
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
            String signature = OAuth1.createSignature(method, url, parameters, consumerSecret, tokenSecret);
            assertEquals("hCtSmYh+iHYCEqBWrE7C7hYmtUk=", signature);
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
            String result = OAuth1.getOAuth1Header(HttpMethod.POST, url, connectionParameters, queryParameters, nonce, timestamp);
            assertEquals("OAuth oauth_consumer_key=\"xvz1evFS4wEEPTGEFPHBog\", oauth_nonce=\"kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg\", oauth_signature=\"hCtSmYh%2BiHYCEqBWrE7C7hYmtUk%3D\", oauth_signature_method=\"HMAC-SHA1\", oauth_timestamp=\"1318622958\", oauth_token=\"370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb\", oauth_version=\"1.0\"", result);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

}

package istc.bigdawg.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.MyriaException;
import istc.bigdawg.myria.MyriaClient;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.Constants;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class ApiClient {

    public static final String HOST;
    public static final String PORT;
    public static final String CONTENT_TYPE;

    static org.apache.log4j.Logger log = org.apache.log4j.Logger
            .getLogger(ApiClient.class.getName());

    static {
        HOST = BigDawgConfigProperties.INSTANCE.getMyriaHost();
        PORT = BigDawgConfigProperties.INSTANCE.getMyriaPort();
        CONTENT_TYPE = BigDawgConfigProperties.INSTANCE.getMyriaContentType();
        System.out.println(HOST);
        System.out.println(PORT);
        System.out.println(CONTENT_TYPE);
    }

    public static String executeGetQuery(String zipcode) throws ApiException {
        // System.out.println("Execute Myria query.");
        String finalURI;
        try {
            finalURI = String.format(
                    "http://api.openweathermap.org/data/2.5/weather?zip=%s,us&appid=2c96050e66a94faa2eedcaa6663ab2b7",
                    URLEncoder.encode(zipcode, Constants.ENCODING));
            // System.out.println(finalURI);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            log.error("unsupported exception: " + Constants.ENCODING);
            throw new ApiException(e.getMessage(), e);
        }
        GetMethod get = new GetMethod(finalURI);
        HttpClient client = new HttpClient();
        try {
            int returnCode = client.executeMethod(get);
            // System.out.println("return code in startQuery: " + returnCode);
            if (returnCode != 200 && returnCode != 201) {
                String message = "API. Start of the query failed!";
                log.info(message);
                throw new ApiException(message);
            }
            return get.getResponseBodyAsString();
        } catch (HttpException e) {
            e.printStackTrace();
            log.error("Http Excpetion from API.");
            throw new ApiException(e.getMessage(), e);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("IO exception from API.");
            throw new ApiException(e.getMessage(), e);
        }
    }

//    public static String getDataset(String jsonData)
//            throws IOException, ApiException {
//        // System.out.println("jsonData in getDataset: " + jsonData);
//        String url = new ObjectMapper().readTree(jsonData).get("query_status")
//                .get("url").asText()
//                .replace("query/query-", "dataset?queryId=");
//        url = url.replace("localhost", HOST);
//        // System.out.println("URL in getDataset: " + url);
//        GetMethod getMethod = new GetMethod(url);
//        HttpClient client = new HttpClient();
//        int returnCode = client.executeMethod(getMethod);
//        String response = getMethod.getResponseBodyAsString();
//
//        if (returnCode != 200 && returnCode != 201)
//            throw new ApiException(
//                    "Failed : HTTP error code : " + returnCode + response);
//        return response;
//    }
}

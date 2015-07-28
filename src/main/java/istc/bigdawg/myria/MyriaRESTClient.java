package istc.bigdawg.myria;

import java.io.IOException;
import java.net.URLEncoder;
//import com.sun.jersey.api.client.Client;
//import com.sun.jersey.api.client.ClientResponse;
//import com.sun.jersey.api.client.WebResource;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
 
public class MyriaRESTClient {
//    public static void main(String[] args) throws IOException {
//	String coordinator = "node-037";
//	int port = 8090;
//	String query = "T1 = empty(x:int); T2 = [from T1 emit $0 as x]; store(T2, JustX);";
//
//	String response = execute(coordinator, port, query);
//	if(waitForCompletion(coordinator, response).equals("SUCCESS"))
//	    System.out.println(getDataset(coordinator, response));
//    }
//
//    public static String execute(String coordinator, int port, String query) throws IOException {
//	Client client = Client.create();
// 
//	// NOTE: may have to move this to the request body if query gets too long
//	WebResource webResource = client
//	    .resource(String.format("http://%s:%d/execute?language=myrial&query=%s", 
//				    coordinator, port, URLEncoder.encode(query, "UTF-8")));
// 
//	ClientResponse response = webResource
//            .accept("application/json")
//	    .post(ClientResponse.class);
// 
//	if (response.getStatus() != 200 && response.getStatus() != 201)
//	    throw new RuntimeException("Failed : HTTP error code : "
//	   			       + response.getStatus() 
//				       + response.getEntity(String.class));
// 
//	return response.getEntity(String.class);
//    }
//
//    public static String waitForCompletion(String coordinator, String jsonData) throws IOException {
//	String status;
//	Client client = Client.create();
//	String url = new ObjectMapper().readTree(jsonData).path("url").asText();
//	url = url.replace("localhost", coordinator); // Temporary hack, will probably be fixed
//
//	do {
//	    ClientResponse response = client.resource(url).accept("application/json").get(ClientResponse.class);
//	    status = new ObjectMapper().readTree(response.getEntity(String.class)).path("status").asText();
//	} while(status.equals("RUNNING") || status.equals("ACCEPTED"));
//
//	return status;
//    }
//
//    public static String getDataset(String coordinator, String jsonData) throws IOException {
//        Client client = Client.create();
//        String url = new ObjectMapper().readTree(jsonData)
//	    .path("url").asText()
//	    .replace("query/query-", "dataset?queryId=");
//        url = url.replace("localhost", coordinator); // Temporary hack, will be fixed                                         
//	WebResource webResource = client
//            .resource(url);
//
//        ClientResponse response = webResource
//            .accept("application/json")
//            .get(ClientResponse.class);
//
//        if (response.getStatus() != 200 && response.getStatus() != 201)
//            throw new RuntimeException("Failed : HTTP error code : "
//                                       + response.getStatus()
//                                       + response.getEntity(String.class));
//
//        return response.getEntity(String.class);
//    }
}

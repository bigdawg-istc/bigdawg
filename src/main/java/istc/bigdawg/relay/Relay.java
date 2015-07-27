/**
 * 
 */
package istc.bigdawg.relay;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * @author Adam Dziedzic
 * 
 */
@Path("/")
public class Relay {

	public static final String RELAY_URL = "RelayURL";

	static org.apache.log4j.Logger log = org.apache.log4j.Logger
			.getLogger(Relay.class.getName());

	@Path("from")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response query(@Context HttpHeaders headers, String istream) throws HttpException, IOException {
		log.info("istream: " + istream);
		JSONObject object = (JSONObject) JSONValue.parse(istream.toString());
		String uri=object.get(RELAY_URL).toString();
		object.remove(RELAY_URL);
		PostMethod post = new PostMethod(uri);
		StringRequestEntity requestEntity = new StringRequestEntity(object.toString(), "application/json", "UTF-8");
		post.setRequestEntity(requestEntity);
		String response;
		HttpClient client = new HttpClient();
		int returnCode = client.executeMethod(post);
		try {
			response = post.getResponseBodyAsString();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(returnCode).entity("No body returned!").build();
		}
		return Response.status(returnCode).entity(response).build();
	}
	
	public void addHeaderParameters(PostMethod post, HttpHeaders headers) {
		MultivaluedMap<String, String> headerKeyValues = headers.getRequestHeaders();
		System.out.println("Header");
		for (java.util.Map.Entry<String, List<String>> entry : headerKeyValues.entrySet()) {
			System.out.println("key:"+entry.getKey()+" value:"+entry.getValue().get(0));
			post.addRequestHeader(entry.getKey(),entry.getValue().get(0));
		}
	}
	
	public void showParameters(PostMethod post, JSONObject object ) {
		Set<String> keySet = object.keySet();
		System.out.println("Body");
		for (String key : keySet) {
			Object value = object.get(key);
			System.out.printf("%s=%s (%s)\n", key, value.toString(), value.getClass()
					.getSimpleName());
		}
	}

	public Relay() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String jsonString = "{\"Query\":\"checkHeartRate\",\"Authorization\":{}, \"NotifyURL\":\"http://localhost:8008/results\", \"OneTime\":\"True\",\"RelayURL\":\"http://cambridge.cs.pdx.edu:8080/test\"}";
		Relay relay = new Relay();
		HttpHeaders headers = new HttpHeaders() {
			
			@Override
			public MultivaluedMap<String, String> getRequestHeaders() {
				MultivaluedMap<String, String> map = new MultivaluedHashMap<String, String>();
				map.add("Content-Type", "application/json");
				return map;
			}
			
			@Override
			public List<String> getRequestHeader(String name) {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public MediaType getMediaType() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public int getLength() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public Locale getLanguage() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public String getHeaderString(String name) {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Date getDate() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Map<String, Cookie> getCookies() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public List<MediaType> getAcceptableMediaTypes() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public List<Locale> getAcceptableLanguages() {
				// TODO Auto-generated method stub
				return null;
			}
		};
		Response response;
		try {
			response = relay.query(headers, jsonString);
			System.out.println(response.getEntity());
		} catch (HttpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

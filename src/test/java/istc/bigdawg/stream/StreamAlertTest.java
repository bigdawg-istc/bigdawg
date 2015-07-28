package istc.bigdawg.stream;

import java.io.IOException;

import javax.validation.constraints.AssertTrue;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StreamAlertTest {

	public StreamAlertTest() {
		// TODO Auto-generated constructor stub
	}
	
	String samplePost = "{\"data\":[{\"PATIENT_ID\":3,\"TS\":4880,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"},{\"PATIENT_ID\":2,\"TS\":76616,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"},{\"PATIENT_ID\":3,\"TS\":85192,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"},{\"PATIENT_ID\":3,\"TS\":95200,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"},{\"PATIENT_ID\":2,\"TS\":96624,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"}],\"error\":\"\",\"success\":1}";
	String samplePost2 = "{\"data\":[{\"PATIENT_ID\":2,\"TS\":76616,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"},{\"PATIENT_ID\":3,\"TS\":85192,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"},{\"PATIENT_ID\":3,\"TS\":95200,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"},{\"PATIENT_ID\":2,\"TS\":96624,\"SIGNAME\":\"ABP\",\"INTERVAL\":5,\"ALERT_MSG\":\"\",\"ACTION_MSG\":\"\"}],\"error\":\"\",\"success\":1}";

	@Test
	public void testJSON() throws JsonParseException, JsonMappingException, IOException, ParseException {
		ObjectMapper mapper = new ObjectMapper();
		//SStoreAlertRequest req = mapper.readValue(samplePost, SStoreAlertRequest.class);
		System.out.println(samplePost);
		JSONObject json = (JSONObject)new JSONParser().parse(samplePost);
		assertTrue(json!=null);
		System.out.println(json.toString());
		JSONArray data = (JSONArray) json.get("data");
		assertTrue(data != null);
		System.out.println(data.toString());	
	}

	@Test
	public void testJSONMerge() throws JsonParseException, JsonMappingException, IOException, ParseException {
		ObjectMapper mapper = new ObjectMapper();
		//SStoreAlertRequest req = mapper.readValue(samplePost, SStoreAlertRequest.class);
		JSONObject json = (JSONObject)new JSONParser().parse(samplePost);
		assertTrue(json!=null);
		JSONArray data = (JSONArray) json.get("data");
		assertTrue(data != null);
		JSONObject json2 = (JSONObject)new JSONParser().parse(samplePost2);
		assertTrue(json2!=null);
		JSONArray data2 = (JSONArray) json2.get("data");
		assertTrue(data2 != null);
		int d1 = data.size();
		int d2 = data2.size();
//		System.out.println(data);
//		System.out.println(data.size());
		data.addAll(data2);
//		System.out.println(data);
//		System.out.println(data.size());
		assertTrue((d1+d2)==data.size());
		
		
	}
	
}

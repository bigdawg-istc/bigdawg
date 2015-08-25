package istc.bigdawg.stream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import istc.bigdawg.AuthorizationRequest;
import istc.bigdawg.exceptions.AlertException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.stream.StreamDAO.DBAlert;
import istc.bigdawg.stream.StreamDAO.PushNotify;

public class AlertManager {
	static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(AlertManager.class.getName());

	public static void RegisterDBEvent(DBAlert alert) throws AlertException {

		ObjectMapper mapper = new ObjectMapper();

		String ExecutionInfo = "Context: Registering DB Event in SStore. ";

		RegisterStreamRequest sStoreRequest = new RegisterStreamRequest();
		sStoreRequest.setAuthorization(new AuthorizationRequest());
		sStoreRequest.setNotifyURL(alert.receiveURL);
		sStoreRequest.setOneTime(alert.oneTime);
		sStoreRequest.setQuery(alert.stroredProc);
		String reqString = null;
		try {
			reqString = mapper.writeValueAsString(sStoreRequest);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			String message = "Problem with Json processing. " + ExecutionInfo + " " + e.getMessage();
			log.error(message);
			throw new AlertException(message);
		}
		// System.out.println("REQ: "+ reqString);
		PostMethod post = new PostMethod(BigDawgConfigProperties.INSTANCE.getsStoreURL());
		// System.out.println("Post to" + post.toString());
		StringRequestEntity requestEntity = null;
		String encoding = "UTF-8";
		try {
			requestEntity = new StringRequestEntity(reqString, "application/json", encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			String message = "Problem with encoding " + encoding + " " + ExecutionInfo + e.getMessage();
			log.error(message);
			throw new AlertException(message);
		}
		post.setRequestEntity(requestEntity);
		HttpClient client = new HttpClient();
		int returnCode = 0;
		try {
			returnCode = client.executeMethod(post);
		} catch (IOException e) {
			e.printStackTrace();
			String message = "Problem with post method execution. " + "URL: "
					+ BigDawgConfigProperties.INSTANCE.getsStoreURL() + " " + ExecutionInfo + e.getMessage();
			log.error(message);
			throw new AlertException(message);
		}
		if (returnCode != 200) {
			String body = "not available";
			try {
				body = post.getResponseBody().toString();
			} catch (IOException e) {
				e.printStackTrace();
			}
			String message = "Bad response. Return Code: " + returnCode + " Post: " + post.toString() + " Body: "
					+ body;
			log.error(message);
			throw new AlertException(message);
		}

	}

	public static void PushEvents(List<PushNotify> urls) {

		HttpClient client = new HttpClient();
		for (PushNotify u : urls) {
			GetMethod get = new GetMethod(u.url);
			try {
				// TODO post method

				System.out.println("Calling : " + u + " need to add: " + u.body);
				int returnCode = client.executeMethod(get);
				System.out.println(returnCode);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

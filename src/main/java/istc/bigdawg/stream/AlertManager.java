package istc.bigdawg.stream;

import istc.bigdawg.AuthorizationRequest;
import istc.bigdawg.exceptions.AlertException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.stream.StreamDAO.DBAlert;
import istc.bigdawg.stream.StreamDAO.PushNotify;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AlertManager {


	public static void RegisterDBEvent(DBAlert alert) throws AlertException{
		ObjectMapper mapper = new ObjectMapper();
		try{
			RegisterStreamRequest sStoreRequest = new RegisterStreamRequest();
			sStoreRequest.setAuthorization(new AuthorizationRequest());
			sStoreRequest.setNotifyURL(alert.receiveURL);
			sStoreRequest.setOneTime(alert.oneTime);
			sStoreRequest.setQuery(alert.stroredProc);
			String reqString = mapper.writeValueAsString(sStoreRequest);
//			System.out.println("REQ: "+ reqString);
			PostMethod post = new PostMethod(BigDawgConfigProperties.INSTANCE.getsStoreURL());
//			System.out.println("Post to" + post.toString());
			StringRequestEntity requestEntity = new StringRequestEntity(
					reqString, "application/json", "UTF-8");
			post.setRequestEntity(requestEntity);
			HttpClient client = new HttpClient();
			int returnCode = client.executeMethod(post);
			if (returnCode != 200){
				throw new AlertException();
			}
		} catch (Exception ex){
			ex.printStackTrace();
			throw new AlertException(ex);
		}
		
	}
	public static void PushEvents(List<PushNotify> urls){

		HttpClient client = new HttpClient();
		for (PushNotify u: urls){
			GetMethod get = new GetMethod(u.url);
			try {
				//TODO post method
				
				System.out.println("Calling : "+u + " need to add: " + u.body);
				int returnCode = client.executeMethod(get);
				System.out.println(returnCode);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}

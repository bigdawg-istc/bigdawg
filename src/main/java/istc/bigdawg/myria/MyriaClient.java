package istc.bigdawg.myria;

import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.Constants;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

public class MyriaClient {

	public static final String URI;
	public static final String CONTENT_TYPE;
	
	static {
		URI = BigDawgConfigProperties.INSTANCE.getMyriaURL();
		CONTENT_TYPE = BigDawgConfigProperties.INSTANCE.getMyriaContentType();
		System.out.println(URI);
		System.out.println(CONTENT_TYPE);
	}

	public static String getResult(String query) throws IOException {
		System.out.println("Execute Myria query.");
		PostMethod post = new PostMethod(URI);
		StringRequestEntity requestEntity = new StringRequestEntity(query,
				CONTENT_TYPE, Constants.ENCODING);
		post.setRequestEntity(requestEntity);
		HttpClient client = new HttpClient();
		int returnCode = client.executeMethod(post);
		System.out.println(returnCode);
		String response = post.getResponseBodyAsString();
		return response;
	}

	public static void main(String[] arg) {
		System.out.println("Myria");
		String result;
		try {
			result = getResult("T1 = empty(x:int); T2 = [from T1 emit $0 as x]; store(T2, JustX);");
			System.out.println(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

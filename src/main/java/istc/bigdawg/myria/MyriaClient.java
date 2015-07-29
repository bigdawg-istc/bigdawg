package istc.bigdawg.myria;

import istc.bigdawg.exceptions.MyriaException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.Constants;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MyriaClient {

	public static final String HOST;
	public static final String PORT;
	public static final String CONTENT_TYPE;

	static org.apache.log4j.Logger log = org.apache.log4j.Logger
			.getLogger(MyriaClient.class.getName());

	static {
		HOST = BigDawgConfigProperties.INSTANCE.getMyriaHost();
		PORT = BigDawgConfigProperties.INSTANCE.getMyriaPort();
		CONTENT_TYPE = BigDawgConfigProperties.INSTANCE.getMyriaContentType();
		System.out.println(HOST);
		System.out.println(PORT);
		System.out.println(CONTENT_TYPE);
	}

	public static String execute(String query)
			throws IOException, MyriaException {
		String startQueryResponse = startQuery(query);
		if (waitForCompletion(startQueryResponse).equals("SUCCESS"))
			return getDataset(startQueryResponse);
		else {
			return "Myria. Query failed!";
		}
	}

	public static String startQuery(String query) throws IOException, MyriaException {
		System.out.println("Execute Myria query.");
		String finalURI;
		try {
			finalURI = String.format(
					"http://%s:%s/execute?language=myrial&query=%s", HOST,
					PORT, URLEncoder.encode(query, Constants.ENCODING));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			log.error("unsupported exception: " + Constants.ENCODING);
			throw e;
		}
		PostMethod post = new PostMethod(finalURI);
		StringRequestEntity requestEntity;
		requestEntity = new StringRequestEntity(query, CONTENT_TYPE,
				Constants.ENCODING);
		post.setRequestEntity(requestEntity);
		HttpClient client = new HttpClient();
		int returnCode;
		try {
			returnCode = client.executeMethod(post);
			if (returnCode != 200 || returnCode != 201) {
				String message = "Myria. Start of the query failed!";
				log.info(message);
				throw new MyriaException(message);
			}
			System.out.println("Return code: " + returnCode);
			String response = post.getResponseBodyAsString();
			return response;
		} catch (HttpException e) {
			e.printStackTrace();
			log.error("Http Excpetion from Myria.");
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			log.error("IO exception from Myria.");
			throw e;
		}
	}

	public static String waitForCompletion(String jsonData)
			throws IOException {
		String status;
		String url = new ObjectMapper().readTree(jsonData).path("url").asText();
		url = url.replace("localhost", HOST);
		do {
			GetMethod getMethod = new GetMethod(url);
			HttpClient client = new HttpClient();
			int returnCode = client.executeMethod(getMethod);
			String response = getMethod.getResponseBodyAsString();
			status = new ObjectMapper().readTree(response).path("status")
					.asText();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				log.info("Myria thread interrupted.");
			}
		} while (status.equals("RUNNING") || status.equals("ACCEPTED"));
		return status;
	}

	public static String getDataset( String jsonData)
			throws IOException, MyriaException {
		String url = new ObjectMapper().readTree(jsonData).path("url").asText()
				.replace("query/query-", "dataset?queryId=");
		url = url.replace("localhost", HOST);
				GetMethod getMethod = new GetMethod(url);
			HttpClient client = new HttpClient();
			int returnCode = client.executeMethod(getMethod);
			String response = getMethod.getResponseBodyAsString();

		if (returnCode != 200 && returnCode != 201)
			throw new MyriaException("Failed : HTTP error code : "
					+ returnCode +response);
		return response;
	}

	public static void main(String[] arg) {
		System.out.println("Myria");
		String result;
			try {
				result = startQuery("T1 = empty(x:int); T2 = [from T1 emit $0 as x]; store(T2, JustX);");

			System.out.println(result);
						} catch (MyriaException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

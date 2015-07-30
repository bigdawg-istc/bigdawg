package istc.bigdawg.myria;

import istc.bigdawg.exceptions.MyriaException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.JSONValidator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import javax.ws.rs.Path;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import com.fasterxml.jackson.databind.JsonNode;
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

	public static String execute(String query) throws MyriaException {
		String startQueryResponse;
		try {
			startQueryResponse = startQuery(query);
			if (waitForCompletion(startQueryResponse).equals("SUCCESS"))
				return getDataset(startQueryResponse);
			else {
				return "Myria. Query failed!";
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new MyriaException(e.getMessage());
		}
	}

	public static String startQuery(String query) throws IOException,
			MyriaException {
		//System.out.println("Execute Myria query.");
		String finalURI;
		try {
			finalURI = String.format(
					"http://%s:%s/execute?language=myrial&query=%s", HOST,
					PORT, URLEncoder.encode(query, Constants.ENCODING));
			//System.out.println(finalURI);
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
		try {
			int returnCode = client.executeMethod(post);
			//System.out.println("return code in startQuery: " + returnCode);
			if (returnCode != 200 && returnCode != 201) {
				String message = "Myria. Start of the query failed!";
				log.info(message);
				throw new MyriaException(message);
			}
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

	public static String waitForCompletion(String jsonData) throws IOException {
		String status;
		//System.out.println("json data in waitForCompletion: " + jsonData);
		//System.out.println("is json valid: "
		//		+ JSONValidator.isJSONValid(jsonData));

		JsonNode tree = new ObjectMapper().readTree(jsonData);
		JsonNode node = tree.get("query_status").get("url");
		String url = node.asText();
		url = url.replace("localhost", HOST);
		//System.out.println("Myria url for waitForCompletion: " + url);
		do {
			GetMethod getMethod = new GetMethod(url);
			HttpClient client = new HttpClient();
			int returnCode = client.executeMethod(getMethod);
//			System.out.println("Return code for waitForCompletion: "
//					+ returnCode);
			String response = getMethod.getResponseBodyAsString();
			status = new ObjectMapper().readTree(response).path("status")
					.asText();
//			System.out.println("Status of the query in waitForCompletion:"
//					+ status);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				log.info("Myria thread interrupted.");
			}
		} while (status.equals("RUNNING") || status.equals("ACCEPTED"));
		return status;
	}

	public static String getDataset(String jsonData) throws IOException,
			MyriaException {
//		System.out.println("jsonData in getDataset: " + jsonData);
		String url = new ObjectMapper().readTree(jsonData).get("query_status")
				.get("url").asText()
				.replace("query/query-", "dataset?queryId=");
		url = url.replace("localhost", HOST);
//		System.out.println("URL in getDataset: " + url);
		GetMethod getMethod = new GetMethod(url);
		HttpClient client = new HttpClient();
		int returnCode = client.executeMethod(getMethod);
		String response = getMethod.getResponseBodyAsString();

		if (returnCode != 200 && returnCode != 201)
			throw new MyriaException("Failed : HTTP error code : " + returnCode
					+ response);
		return response;
	}

	public static void main(String[] arg) {
		System.out.println("Myria");
		String result;
		try {
			result = execute("T1 = empty(x:int); T2 = [from T1 emit $0 as x]; store(T2, JustX);");

			System.out.println(result);
		} catch (MyriaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

/**
 * 
 */
package istc.bigdawg.utils;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;

/**
 * @author adam
 * 
 */
public class TestGetMethod {

	/**
	 * 
	 */
	public TestGetMethod() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String url = "http://localhost:8080/bigdawg/myresource";
		GetMethod getMethod = new GetMethod(url);
		HttpClient client = new HttpClient();
		try {
			int returnCode = client.executeMethod(getMethod);
			System.out.println("return code: "+returnCode);
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			String response = getMethod.getResponseBodyAsString();
			System.out.println("response: " +response);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

/**
 * 
 */
package istc.bigdawg.utils;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * @author adam
 * 
 */
public class JSONValidator {

	/**
	 * 
	 */
	public JSONValidator() {
		// TODO Auto-generated constructor stub
	}

	public static boolean isJSONValid(String jsonData) {
		try {
			new JSONObject(jsonData);
		} catch (JSONException ex) {
			try {
				new JSONArray(jsonData);
			} catch (JSONException ex1) {
				return false;
			}
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(isJSONValid("{\"adam\":\"dziedzic\"}"));
		System.out.println(isJSONValid("[\"adam\",\"dziedzic\"]"));
	}

}

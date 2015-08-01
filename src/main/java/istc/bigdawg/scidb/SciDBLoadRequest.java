/**
 * 
 */
package istc.bigdawg.scidb;

import istc.bigdawg.AuthorizationRequest;
import istc.bigdawg.interfaces.Request;

/**
 * @author adam
 *
 */
public class SciDBLoadRequest implements Request {

	private String script;
	private AuthorizationRequest authorization;
	private String dataLocation;
	private String flatArrayName;
	private String arrayName;
	
	/**
	 * 
	 */
	public SciDBLoadRequest() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * @return the script
	 */
	public String getScript() {
		return script;
	}

	/**
	 * @param script the script to set
	 */
	public void setScript(String script) {
		this.script = script;
	}

	/**
	 * @return the dataPath
	 */
	public String getDataLocation() {
		return dataLocation;
	}

	/**
	 * @param dataPath the dataPath to set
	 */
	public void setDataLocation(String dataPath) {
		this.dataLocation = dataPath;
	}

	/**
	 * @return the flatArrayName
	 */
	public String getFlatArrayName() {
		return flatArrayName;
	}

	/**
	 * @param flatArrayName the flatArrayName to set
	 */
	public void setFlatArrayName(String flatArrayName) {
		this.flatArrayName = flatArrayName;
	}

	/**
	 * @return the arrayName
	 */
	public String getArrayName() {
		return arrayName;
	}

	/**
	 * @param arrayName the arrayName to set
	 */
	public void setArrayName(String arrayName) {
		this.arrayName = arrayName;
	}

	/**
	 * @return the authorization
	 */
	public AuthorizationRequest getAuthorization() {
		return authorization;
	}

	/**
	 * @param authorization the authorization to set
	 */
	public void setAuthorization(AuthorizationRequest authorization) {
		this.authorization = authorization;
	}
	
}

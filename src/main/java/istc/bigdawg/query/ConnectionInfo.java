/**
 * 
 */
package istc.bigdawg.query;

/**
 * @author Adam Dziedzic
 * 
 * 
 *
 */
public interface ConnectionInfo {

	/**
	 * 
	 * @return name of the host where the instance is running from which the
	 *         connection will be obtained
	 */
	public String getHost();

	/**
	 * 
	 * @return port number of the running instance from which the connection
	 *         will be obtained
	 */
	public String getPort();

	/**
	 * 
	 * @return user name who can can get the connection
	 */
	public String getUser();
	
	
	/**
	 * 
	 * @return password for the user
	 */
	public String getPassword();
	
}

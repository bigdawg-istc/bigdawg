/**
 * 
 */
package istc.bigdawg.postgresql;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class PostgreSQLConnectionInfo {

	private String host;
	private String port;
	private String database;
	private String user;
	private String password;

	public PostgreSQLConnectionInfo(String host, String port, String database, String user, String password) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.user = user;
		this.password = password;
	}
	
	public String getUrl() {
		return "jdbc:postgresql://"+getHost()+":"+getPort()+"/"+getDatabase();
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}

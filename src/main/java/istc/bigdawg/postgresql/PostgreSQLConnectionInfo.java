/**
 * 
 */
package istc.bigdawg.postgresql;

import java.util.Collection;

import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class PostgreSQLConnectionInfo implements ConnectionInfo {

	private String host;
	private String port;
	private String database;
	private String user;
	private String password;
	
	private static final String CLEANUP_STRING = "DROP TABLE %s;";

	public PostgreSQLConnectionInfo(String host, String port, String database, String user, String password) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.user = user;
		this.password = password;
	}

	public String getUrl() {
		return "jdbc:postgresql://" + getHost() + ":" + getPort() + "/" + getDatabase();
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * This is a specific property for PostgreSQL database.
	 * 
	 * @return the database name to which the connection should be established
	 */
	public String getDatabase() {
		return database;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getHost()
	 */
	@Override
	public String getHost() {
		return host;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getPort()
	 */
	@Override
	public String getPort() {
		return port;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getUser()
	 */
	@Override
	public String getUser() {
		return user;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getPassword()
	 */
	@Override
	public String getPassword() {
		return password;
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
    String NEW_LINE = System.getProperty("line.separator");

    result.append(this.getClass().getName() + " Object {" + NEW_LINE);
    result.append(" Host: " + this.getHost() + NEW_LINE);
    result.append(" Port: " + this.getPort() + NEW_LINE);
    result.append(" Database: " + this.getDatabase() + NEW_LINE );
    result.append(" User: " + this.getUser() + NEW_LINE);
    result.append(" Password: secret" + NEW_LINE);
    result.append("}");

    return result.toString();
	}

    @Override
    public String getCleanupQuery(Collection<String> objects) {
        return String.format(CLEANUP_STRING, String.join(", ", objects));
    }

    @Override
    public DBHandler getHandler() {
        return new PostgreSQLHandler(this);
    }

}

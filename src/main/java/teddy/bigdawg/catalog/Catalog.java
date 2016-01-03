package teddy.bigdawg.catalog;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Connection and setup of the catalog
 * @author Jack
 *
 */
public class Catalog {
	
	protected Connection connection;
	protected Statement stmt;
	protected boolean initiated;
	protected boolean connected;
	private String lastURL			= null;
	private String lastUsername		= null;
	private String lastPassword		= null;
	
	/**
	 * This constructor doesn't do much. Catalog requires explicit call to 
	 * connect(*) to work.
	 */
	public Catalog () {
		initiated = false;
		connected = false;
	}
	
	public void commit() throws SQLException{
		connection.commit();
	}
	
	public void execNoRet(String str) throws SQLException{
		stmt.executeUpdate(str);
	}
	
	public ResultSet execRet(String str) throws SQLException {
		return stmt.executeQuery(str);
	}
	
	public boolean isConnected() {
		return this.connected;
	}
	
	public boolean isInitiated() {
		return this.initiated;
	}

	public String getLastURL() {
		return lastURL;
	}

	public void setLastURL(String lastURL) {
		this.lastURL = lastURL;
	}

	public String getLastUsername() {
		return lastUsername;
	}

	public void setLastUsername(String lastUsername) {
		this.lastUsername = lastUsername;
	}

	public String getLastPassword() {
		return lastPassword;
	}

	public void setLastPassword(String lastPassword) {
		this.lastPassword = lastPassword;
	}
}
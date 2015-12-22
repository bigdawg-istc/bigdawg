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
}
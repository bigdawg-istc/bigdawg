/**
 * 
 */
package istc.bigdawg.postgresql;

import istc.bigdawg.properties.BigDawgConfigProperties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Adam Dziedzic
 *
 */
public enum PostgreSQLInstance {
	
	INSTANCE;
	
	public static final String URL;
	public static final String USER;
	public static final String PASSWORD;
	
	private PostgreSQLInstance() {
	}

	static {
		URL = BigDawgConfigProperties.INSTANCE.getPostgreSQLURL();
		USER= BigDawgConfigProperties.INSTANCE.getPostgreSQLUser();
		PASSWORD = BigDawgConfigProperties.INSTANCE
				.getPostgreSQLPassword();
	}
	
	public static Connection getConnection() throws SQLException {
		// https://jdbc.postgresql.org/documentation/head/connect.htlm
		Properties props = new Properties();
		props.setProperty("user", USER);
		props.setProperty("password", PASSWORD);
		// do not cache queries
		props.setProperty("preparedStatementCacheQueries", "0");
		props.setProperty("preparedStatementCacheSizeMiB", "10");
//		System.out.println("Connection URL: "+URL);
		
		return  DriverManager.getConnection(URL, props);
	}

}

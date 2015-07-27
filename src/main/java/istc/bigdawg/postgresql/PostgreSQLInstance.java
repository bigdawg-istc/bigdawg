/**
 * 
 */
package istc.bigdawg.postgresql;

import istc.bigdawg.properties.BigDawgConfigProperties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
		return  DriverManager.getConnection(URL, USER, PASSWORD);
	}

}

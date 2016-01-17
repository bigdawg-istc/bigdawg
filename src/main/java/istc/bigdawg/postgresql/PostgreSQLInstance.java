/**
 * 
 */
package istc.bigdawg.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * @author Adam Dziedzic
 *
 */
public enum PostgreSQLInstance {

	INSTANCE;

	private static Logger log = Logger.getLogger(PostgreSQLInstance.class.getName());

	public static final String URL;
	public static final String USER;
	public static final String PASSWORD;

	private PostgreSQLInstance() {
	}

	static {
		URL = BigDawgConfigProperties.INSTANCE.getPostgreSQLURL();
		USER = BigDawgConfigProperties.INSTANCE.getPostgreSQLUser();
		PASSWORD = BigDawgConfigProperties.INSTANCE.getPostgreSQLPassword();
	}

	public static Connection getConnection() throws SQLException {
		// https://jdbc.postgresql.org/documentation/head/connect.htlm
		Properties props = new Properties();
		props.setProperty("user", USER);
		props.setProperty("password", PASSWORD);
		// do not cache queries
		//props.setProperty("preparedStatementCacheQueries", "0");
		//props.setProperty("preparedStatementCacheSizeMiB", "10");
		try {
			return DriverManager.getConnection(URL, props);
		} catch (SQLException e) {
			log.error(e.getMessage() + " Could not establish connection to PostgreSQL using the property file: "
					+ getStringRepresentation(), e);
			e.printStackTrace();
			throw e;
		}
	}

	public static String getStringRepresentation() {
		return "URL: " + URL + " USER: " + USER + " PASSWORD: this is not supposed to be displayed here";
	}
}

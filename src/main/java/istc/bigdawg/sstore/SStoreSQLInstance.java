package istc.bigdawg.sstore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import istc.bigdawg.properties.BigDawgConfigProperties;

public enum SStoreSQLInstance {
    INSTANCE;
    
    private static Logger log = Logger.getLogger(SStoreSQLInstance.class.getName());

	public static final String URL;
	public static final String USER;
	public static final String PASSWORD;

	private SStoreSQLInstance() {
	}

	static {
		URL = BigDawgConfigProperties.INSTANCE.getSStoreSQLurl();
		USER = BigDawgConfigProperties.INSTANCE.getSStoreSQLUser();
		PASSWORD = BigDawgConfigProperties.INSTANCE.getSStoreSQLPassword();
	}

	public static Connection getConnection() throws SQLException {
		Properties props = new Properties();
		props.setProperty("user", USER);
		props.setProperty("password", PASSWORD);
		try {
			return DriverManager.getConnection(URL, props);
		} catch (SQLException e) {
			log.error(e.getMessage() + " Could not establish connection to SStore using the property file: "
					+ getStringRepresentation(), e);
			e.printStackTrace();
			throw e;
		}
	}

	public static String getStringRepresentation() {
		return "URL: " + URL + " USER: " + USER + " PASSWORD: this is not supposed to be displayed here";
	}
}

package istc.bigdawg.properties;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public enum BigDawgConfigProperties {
	INSTANCE;

	private String grizzlyIpAddress;
	private String grizzlyPort;

	private String postgreSQLURL;
	private String postgreSQLUser;
	private String postgreSQLPassword;

	private String accumuloIstance;
	private String accumuloZooKeepers;
	private String accumuloUser;
	private String accumuloPasswordToken;

	private String scidbHost;
	private String scidbPort;
	private String scidbUser;
	private String scidbPassword;

	private String sStoreURL;
	private String accumuloShellScript;

	private BigDawgConfigProperties() throws AssertionError {
		Properties prop = new Properties();
		String propFileName = "bigdawg-config.properties";
		InputStream inputStream = BigDawgConfigProperties.class
				.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream != null) {
			try {
				prop.load(inputStream);
			} catch (IOException e) {
				e.printStackTrace();
				throw new AssertionError(e);
			}
		} else {
			FileNotFoundException e = new FileNotFoundException(
					"property file '" + propFileName
							+ "' not found in the classpath");
			e.printStackTrace();
			throw new AssertionError(e);
		}
		this.grizzlyIpAddress = prop.getProperty("main.grizzly.ipaddress");
		this.grizzlyPort = prop.getProperty("main.grizzly.port");

		this.postgreSQLURL = prop.getProperty("main.postgresql.url");
		this.postgreSQLUser = prop.getProperty("main.postgresql.user");
		this.postgreSQLPassword = prop.getProperty("main.postgresql.password");

		this.accumuloIstance = prop.getProperty("main.accumulo.instanceName");
		this.accumuloZooKeepers = prop.getProperty("main.accumulo.zooKeepers");
		this.accumuloUser = prop.getProperty("main.accumulo.user");
		this.accumuloPasswordToken = prop
				.getProperty("main.accumulo.passwordToken");
		this.accumuloShellScript=prop.getProperty("main.accumulo.shell.script");
		
		this.sStoreURL = prop.getProperty("main.sstore.alerturl");
		
	}
	/**
	 * @return the accumuloIstance
	 */
	public String getAccumuloIstance() {
		return accumuloIstance;
	}

	/**
	 * @return the accumuloZooKeepers
	 */
	public String getAccumuloZooKeepers() {
		return accumuloZooKeepers;
	}

	/**
	 * @return the accumuloUser
	 */
	public String getAccumuloUser() {
		return accumuloUser;
	}

	/**
	 * @return the accumuloPasswordToken
	 */
	public String getAccumuloPasswordToken() {
		return accumuloPasswordToken;
	}

	/**
	 * @return the scidbHost
	 */
	public String getScidbHost() {
		return scidbHost;
	}

	/**
	 * @return the scidbPort
	 */
	public String getScidbPort() {
		return scidbPort;
	}

	/**
	 * @return the scidbUser
	 */
	public String getScidbUser() {
		return scidbUser;
	}

	/**
	 * @return the scidbPassword
	 */
	public String getScidbPassword() {
		return scidbPassword;
	}

	/**
	 * @return the postgreSQLUser
	 */
	public String getPostgreSQLUser() {
		return postgreSQLUser;
	}

	/**
	 * @return the postgreSQLPassword
	 */
	public String getPostgreSQLPassword() {
		return postgreSQLPassword;
	}

	public String getPostgreSQLURL() {
		return postgreSQLURL;
	}

	/**
	 * @return the grizzlyIpAddress
	 */
	public String getGrizzlyIpAddress() {
		return grizzlyIpAddress;
	}

	/**
	 * @return the grizzlyPort
	 */
	public String getGrizzlyPort() {
		return grizzlyPort;
	}

	public String getBaseURI() {
		String baseURI = "http://"
				+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress() + ":"
				+ BigDawgConfigProperties.INSTANCE.getGrizzlyPort()
				+ "/bigdawg/";
		return baseURI;
	}

	public String getsStoreURL() {
		return sStoreURL;
	}

	/**
	 * @return the accumuloShellScript
	 */
	public String getAccumuloShellScript() {
		return accumuloShellScript;
	}

}

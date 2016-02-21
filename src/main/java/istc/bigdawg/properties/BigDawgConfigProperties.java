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

	private String postgreSQLTestHost;
	private String postgreSQLTestPort;
	private String postgreSQLTestDatabase;
	private String postgreSQLTestUser;
	private String postgreSQLTestPassword;

	private String accumuloIstanceType;
	private String accumuloIstanceName;
	private String accumuloZooKeepers;
	private String accumuloUser;
	private String accumuloPasswordToken;

	private String scidbHostname;
	private String scidbPort;
	private String scidbUser;
	private String scidbPassword;
	private String scidbBinPath;

	private String sStoreURL;
	private String accumuloShellScript;

	private String myriaHost;
	private String myriaPort;
	private String myriaContentType;

	private BigDawgConfigProperties() throws AssertionError {
		Properties prop = new Properties();
		String propFileName = "bigdawg-config.properties";
		InputStream inputStream = BigDawgConfigProperties.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream != null) {
			try {
				prop.load(inputStream);
			} catch (IOException e) {
				e.printStackTrace();
				throw new AssertionError(e);
			}
		} else {
			FileNotFoundException e = new FileNotFoundException(
					"property file '" + propFileName + "' not found in the classpath");
			e.printStackTrace();
			throw new AssertionError(e);
		}
		this.grizzlyIpAddress = prop.getProperty("main.grizzly.ipaddress");
		this.grizzlyPort = prop.getProperty("main.grizzly.port");

		this.postgreSQLURL = prop.getProperty("main.postgresql.url");
		this.postgreSQLUser = prop.getProperty("main.postgresql.user");
		this.postgreSQLPassword = prop.getProperty("main.postgresql.password");
		
		this.postgreSQLTestHost = prop.getProperty("main.postgresql.test.host");
		this.postgreSQLTestPort = prop.getProperty("main.postgresql.test.port");
		this.postgreSQLTestDatabase = prop.getProperty("main.postgresql.test.database");
		this.postgreSQLTestUser = prop.getProperty("main.postgresql.test.user");
		this.postgreSQLTestPassword = prop.getProperty("main.postgresql.test.password");

		this.accumuloIstanceType = prop.getProperty("main.accumulo.instanceType");
		this.accumuloIstanceName = prop.getProperty("main.accumulo.instanceName");
		this.accumuloZooKeepers = prop.getProperty("main.accumulo.zooKeepers");
		this.accumuloUser = prop.getProperty("main.accumulo.user");
		this.accumuloPasswordToken = prop.getProperty("main.accumulo.passwordToken");
		this.accumuloShellScript = prop.getProperty("main.accumulo.shell.script");

		this.sStoreURL = prop.getProperty("main.sstore.alerturl");

		this.myriaHost = prop.getProperty("main.myria.host");
		this.myriaPort = prop.getProperty("main.myria.port");
		this.myriaContentType = prop.getProperty("main.myria.content.type");

		this.scidbHostname = prop.getProperty("main.scidb.hostname");
		this.scidbPort = prop.getProperty("main.scidb.port");
		this.scidbPassword = prop.getProperty("main.scidb.password");
		this.scidbUser = prop.getProperty("main.scidb.user");
		this.scidbBinPath = prop.getProperty("main.scidb.bin_path");
	}

	/**
	 * @return the accumuloIstanceType
	 */
	public String getAccumuloIstanceType() {
		return accumuloIstanceType;
	}

	/**
	 * @return the accumuloIstanceType
	 */
	public String getAccumuloIstanceName() {
		return accumuloIstanceName;
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
	public String getScidbHostname() {
		return scidbHostname;
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
		String baseURI = "http://" + BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress() + ":"
				+ BigDawgConfigProperties.INSTANCE.getGrizzlyPort() + "/bigdawg/";
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

	/**
	 * @return the myriaHost
	 */
	public String getMyriaHost() {
		return myriaHost;
	}

	/**
	 * @return the myriaPort
	 */
	public String getMyriaPort() {
		return myriaPort;
	}

	/**
	 * @return the myriaContentType
	 */
	public String getMyriaContentType() {
		return myriaContentType;
	}

	/**
	 * 
	 * @return SciDB bin path
	 */
	public String getScidbBinPath() {
		return scidbBinPath;
	}

	/**
	 * @return the scidbPort
	 */
	public String getScidbPort() {
		return scidbPort;
	}
	
		/**
	 * @return the postgreSQLTestHost
	 */
	public String getPostgreSQLTestHost() {
		return postgreSQLTestHost;
	}

	/**
	 * @return the postgreSQLTestPort
	 */
	public String getPostgreSQLTestPort() {
		return postgreSQLTestPort;
	}

	/**
	 * @return the postgreSQLTestDatabase
	 */
	public String getPostgreSQLTestDatabase() {
		return postgreSQLTestDatabase;
	}

	/**
	 * @return the postgreSQLTestUser
	 */
	public String getPostgreSQLTestUser() {
		return postgreSQLTestUser;
	}

	/**
	 * @return the postgreSQLTestPassword
	 */
	public String getPostgreSQLTestPassword() {
		return postgreSQLTestPassword;
	}
}

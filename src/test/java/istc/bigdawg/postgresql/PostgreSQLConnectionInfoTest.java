/**
 * 
 */
package istc.bigdawg.postgresql;

import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 18, 2016 10:53:36 AM
 */
public class PostgreSQLConnectionInfoTest extends PostgreSQLConnectionInfo {

	/**
	 * @param host
	 * @param port
	 * @param database
	 * @param user
	 * @param password
	 */
	public PostgreSQLConnectionInfoTest() {
		super(BigDawgConfigProperties.INSTANCE.getPostgreSQLTestHost(),
				BigDawgConfigProperties.INSTANCE.getPostgreSQLTestPort(),
				BigDawgConfigProperties.INSTANCE.getPostgreSQLTestDatabase(),
				BigDawgConfigProperties.INSTANCE.getPostgreSQLTestUser(),
				BigDawgConfigProperties.INSTANCE.getPostgreSQLTestPassword());
	}

}

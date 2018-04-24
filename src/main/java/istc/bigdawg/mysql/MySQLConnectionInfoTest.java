/**
 *
 */
package istc.bigdawg.mysql;

import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * @author Kate Yu
 */
public class MySQLConnectionInfoTest extends MySQLConnectionInfo {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param host
     * @param port
     * @param database
     * @param user
     * @param password
     */
    public MySQLConnectionInfoTest() {
        super(BigDawgConfigProperties.INSTANCE.getMySQLTestHost(),
                BigDawgConfigProperties.INSTANCE.getMySQLTestPort(),
                BigDawgConfigProperties.INSTANCE.getMySQLTestDatabase(),
                BigDawgConfigProperties.INSTANCE.getMySQLTestUser(),
                BigDawgConfigProperties.INSTANCE.getMySQLTestPassword());
    }

}

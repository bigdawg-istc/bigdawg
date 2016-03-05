/**
 * 
 */
package istc.bigdawg.scidb;

import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * Connection to SciDB for tests.
 * 
 * @author Adam Dziedzic
 * 
 *         Mar 2, 2016 5:39:15 PM
 */
public class SciDBConnectionInfoTest extends SciDBConnectionInfo {

	/**
	 * 
	 */
	public SciDBConnectionInfoTest() {
		super(BigDawgConfigProperties.INSTANCE.getScidbTestHostname(),
				BigDawgConfigProperties.INSTANCE.getScidbTestPort(),
				BigDawgConfigProperties.INSTANCE.getScidbTestUser(),
				BigDawgConfigProperties.INSTANCE.getScidbTestPassword(),
				BigDawgConfigProperties.INSTANCE.getScidbTestBinPath());
	}

}

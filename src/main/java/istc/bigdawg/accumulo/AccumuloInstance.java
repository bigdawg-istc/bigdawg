/**
 * 
 */
package istc.bigdawg.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.eclipse.jdt.core.dom.ThisExpression;

/**
 * @author Adam Dziedzic
 * 
 */
public class AccumuloInstance {

	private Connector conn;

	private AccumuloInstance(Connector conn) {
		this.conn = conn;
	}

	/**
	 * 
	 */
	public AccumuloInstance(Parameters params) {

	}

	public static AccumuloInstance getMiniCluster() throws AccumuloException, AccumuloSecurityException {
		Instance inst = new ZooKeeperInstance("miniInstance", "localhost:37266");
		try {
			Connector conn = inst.getConnector("root", new PasswordToken("secret"));
			return new AccumuloInstance(conn);
		} catch (AccumuloSecurityException e) {
			System.out.println("Security exception: this should not happen!");
			e.printStackTrace();
			throw e;
		}
	}

	public AccumuloInstance(String instanceName, String zooServers,
			String userName, String password) throws AccumuloException,
			AccumuloSecurityException {
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		conn = inst.getConnector(userName, new PasswordToken(password));
	}

	public Connector getConnector() {
		return conn;
	}

	public boolean createTable(String tableName) {
		TableOperations tabOp = getConnector().tableOperations();
		try {
			tabOp.create(tableName);
			return true;
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableExistsException e1) {
			System.out.println("Table " + tableName + " already exists.");
		}
		return false;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

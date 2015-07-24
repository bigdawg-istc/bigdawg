/**
 * 
 */
package istc.bigdawg.accumulo;

import istc.bigdawg.properties.BigDawgConfigProperties;

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
	private String username;
	private String instanceName;
	private String zooKeepers;
	private String passwordToken;

	/**
	 * @return the conn
	 */
	public Connector getConn() {
		return conn;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @return the instanceName
	 */
	public String getInstanceName() {
		return instanceName;
	}

	/**
	 * @return the zooKeepers
	 */
	public String getZooKeepers() {
		return zooKeepers;
	}

	private AccumuloInstance() {
		
	}
	
	private AccumuloInstance(Connector conn) {
		this.conn = conn;
	}

	/**
	 * 
	 */
	public AccumuloInstance(Parameters params) {

	}

	public static AccumuloInstance getInstance() throws AccumuloException, AccumuloSecurityException {
		AccumuloInstance accInst=new AccumuloInstance();
		accInst.instanceName=BigDawgConfigProperties.INSTANCE.getAccumuloIstance();
		accInst.zooKeepers=BigDawgConfigProperties.INSTANCE.getAccumuloZooKeepers();
		accInst.username=BigDawgConfigProperties.INSTANCE.getAccumuloUser();
		accInst.passwordToken=BigDawgConfigProperties.INSTANCE.getAccumuloPasswordToken();
		Instance inst = new ZooKeeperInstance(accInst.instanceName, accInst.zooKeepers);
		try {
			Connector conn = inst.getConnector(accInst.username, new PasswordToken(accInst.passwordToken));
			accInst.conn=conn;
			return accInst;
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

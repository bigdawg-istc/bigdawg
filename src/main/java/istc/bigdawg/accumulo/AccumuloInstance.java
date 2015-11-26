/**
 * 
 */
package istc.bigdawg.accumulo;

import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * @author Adam Dziedzic
 * 
 */
public class AccumuloInstance {

	public enum InstanceType {
		miniCluster, mockInstance, fullInstance;
	}

	private Connector conn;
	private String username;
	private InstanceType instanceType;
	private String instanceName;
	private String zooKeepers;
	private String passwordToken;

	public static final List<String> fullSchema = Arrays.asList("rowId",
			"colFam", "colKey", "visibility", "value");
	public static final List<String> fullTypes = Arrays.asList("Text", "Text",
			"Text", "Text", "Value");

	public static final List<String> schema = Arrays.asList("rowId", "colKey",
			"value");
	public static final List<String> types = Arrays.asList("Text", "Text",
			"Value");

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
	 * @return the zooKeepers
	 */
	public String getZooKeepers() {
		return zooKeepers;
	}

	/**
	 * @return the instanceType
	 */
	public InstanceType getInstanceType() {
		return instanceType;
	}

	public String getInstanceName() {
		return instanceName;
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

	/**
	 * Set mock instance for development purpose.
	 * 
	 * @param accInst
	 * @return mockInstance of Accumulo
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 */
	private static AccumuloInstance getMockInstance(AccumuloInstance accInst)
			throws AccumuloException, AccumuloSecurityException {
		System.out.println("Started mock instance.");
		Instance inst = new MockInstance(accInst.instanceType.name());
		Connector conn = inst.getConnector("root", new byte[] {});
		accInst.conn = conn;
		return accInst;
	}

	private static AccumuloInstance getFullInstance(AccumuloInstance accInst)
			throws AccumuloException, AccumuloSecurityException {
		accInst.zooKeepers = BigDawgConfigProperties.INSTANCE
				.getAccumuloZooKeepers();
		accInst.username = BigDawgConfigProperties.INSTANCE.getAccumuloUser();
		accInst.passwordToken = BigDawgConfigProperties.INSTANCE
				.getAccumuloPasswordToken();

		// accInst.start();
		Instance inst = new ZooKeeperInstance(accInst.instanceName,
				accInst.zooKeepers);

		try {
			System.out.println("username: " + accInst.username);
			System.out.println("password: " + accInst.passwordToken);

			Connector conn = inst.getConnector(accInst.username,
					new PasswordToken(accInst.passwordToken));
			System.out.println("Connection to Accumulo accepted.");
			// Connector conn = inst.getConnector( "root",new
			// AuthenticationToken("root", "pass", null));
			accInst.conn = conn;
			return accInst;
		} catch (AccumuloSecurityException e) {
			System.out.println("Security exception: this should not happen!");
			e.printStackTrace();
			throw e;
		}
	}

	public static AccumuloInstance getInstance() throws AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException {
		AccumuloInstance accInst = new AccumuloInstance();
		String instanceRawType = BigDawgConfigProperties.INSTANCE
				.getAccumuloIstanceType();
		System.out.println("instanceRawType:"+instanceRawType);
		accInst.instanceType = InstanceType.valueOf(instanceRawType);
		accInst.instanceName = BigDawgConfigProperties.INSTANCE
				.getAccumuloIstanceName();
		if (accInst.instanceType == InstanceType.mockInstance) {
			return getMockInstance(accInst);
		} else if (accInst.instanceType == InstanceType.fullInstance) {
			return getFullInstance(accInst);
		} else {
			String errorMessage = "Unrecognized type of Accumulo instance: "
					+ instanceRawType
					+ " Please, check the system configuration.";
			System.err.println(errorMessage);
			AccumuloBigDawgException exception = new AccumuloBigDawgException(
					errorMessage);
			exception.printStackTrace();
			throw exception;
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

/**
 * 
 */
package istc.bigdawg.accumulo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.jfree.util.Log;

import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 */
public class AccumuloInstance {

	/**
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(AccumuloInstance.class.getName());

	public enum InstanceType {
		miniCluster, mockInstance, fullInstance;
	}

	private Connector conn;
	private String username;
	private InstanceType instanceType;
	private String instanceName;
	private String zooKeepers;
	private String passwordToken;
	private long scanTimeout = 2000; // 2000 ms = 2 seconds
	private int batchSize = 1000;

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
		@SuppressWarnings("deprecation")
		Connector conn = inst.getConnector("root", new byte[] {});
		accInst.conn = conn;
		return accInst;
	}

	public static AccumuloInstance getFullInstance(AccumuloConnectionInfo info)
			throws AccumuloSecurityException, AccumuloException {
		AccumuloInstance accInst = new AccumuloInstance();

		// accInst.start();
		Instance inst = new ZooKeeperInstance(info.getDatabase(),
				info.getHost());

		try {
			// System.out.println("username: " + accInst.username);
			// System.out.println("password: " + accInst.passwordToken);

			Connector conn = inst.getConnector(info.getUser(),
					new PasswordToken(info.getPassword()));
			// System.out.println("Connection to Accumulo accepted.");
			// Connector conn = inst.getConnector( "root",new
			// AuthenticationToken("root", "pass", null));
			accInst.conn = conn;
			return accInst;
		} catch (AccumuloSecurityException e) {
			String msg = "Problem with connection to accumulo - check password.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
			throw e;
		}
	}

	public static AccumuloConnectionInfo getDefaultConnection()
			throws Exception {
		String[] zk = BigDawgConfigProperties.INSTANCE.getZooKeepers()
				.split(":");
		if (zk.length != 2) {
			throw new Exception(
					"The config file should contain zkHost:port data!");
		}
		String zkHost = zk[0];
		String zkPort = zk[1];
		return new AccumuloConnectionInfo(zkHost, zkPort,
				BigDawgConfigProperties.INSTANCE.getAccumuloIstanceName(),
				BigDawgConfigProperties.INSTANCE.getAccumuloUser(),
				BigDawgConfigProperties.INSTANCE.getAccumuloPasswordToken());
	}

	private static AccumuloInstance getFullInstance(AccumuloInstance accInst)
			throws AccumuloException, AccumuloSecurityException {
		accInst.zooKeepers = BigDawgConfigProperties.INSTANCE.getZooKeepers();
		accInst.username = BigDawgConfigProperties.INSTANCE.getAccumuloUser();
		accInst.passwordToken = BigDawgConfigProperties.INSTANCE
				.getAccumuloPasswordToken();

		// accInst.start();
		Instance inst = new ZooKeeperInstance(accInst.instanceName,
				accInst.zooKeepers);

		try {
			// System.out.println("username: " + accInst.username);
			// System.out.println("password: " + accInst.passwordToken);

			Connector conn = inst.getConnector(accInst.username,
					new PasswordToken(accInst.passwordToken));
			// System.out.println("Connection to Accumulo accepted.");
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
		logger.debug("Setup accumulo instance.");
		String instanceRawType = BigDawgConfigProperties.INSTANCE
				.getAccumuloIstanceType();
		logger.debug("instanceRawType:" + instanceRawType);
		accInst.instanceType = InstanceType.valueOf(instanceRawType);
		accInst.instanceName = BigDawgConfigProperties.INSTANCE
				.getAccumuloIstanceName();
		logger.debug("instanceName: " + accInst.instanceName);
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
			String userName, String password)
					throws AccumuloException, AccumuloSecurityException {
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		conn = inst.getConnector(userName, new PasswordToken(password));
	}

	public Connector getConnector() {
		return conn;
	}

	public boolean createTableIfNotExists(String tableName) throws Exception {
		TableOperations tabOp = getConnector().tableOperations();
		if (!tabOp.exists(tableName)) {
			try {
				tabOp.create(tableName);
				return true;
			} catch (AccumuloException | AccumuloSecurityException exp) {
				String msg = "Problem with access to Acccumulo! "
						+ exp.getMessage();
				logger.error(msg + StackTrace.getFullStackTrace(exp));
				throw exp;
			} catch (TableExistsException exp) {
				String msg = "This error should not have happened."
						+ " We checked that the table did not exist and " + ""
						+ "only then wanted to create a new table. "
						+ "Probably a table with the same name "
						+ " was added concurrently. " + exp.getMessage();
				logger.error(msg + StackTrace.getFullStackTrace(exp));
				throw exp;
			}
		}
		return false;
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
			logger.info("Table " + tableName + " already exists.");
		}
		return false;
	}

	public boolean deleteTable(String tableName) {
		TableOperations tabOp = getConnector().tableOperations();
		try {
			tabOp.delete(tableName);
			return true;
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e1) {
			System.out.println("Table " + tableName + " does not exist.");
		}
		return false;
	}

	public Iterator<Entry<Key, Value>> getTableIterator(String table)
			throws TableNotFoundException {
		// Read data: http://bit.ly/1Hoyeqa
		Authorizations authorizations = new Authorizations();
		Scanner scan = conn.createScanner(table, authorizations);
		scan.setBatchSize(batchSize);
		scan.setTimeout(scanTimeout, TimeUnit.MILLISECONDS);
		scan.setRange(new Range()); // get all data sorted - we want to recover
									// rows
		Iterator<Entry<Key, Value>> iter = scan.iterator();
		return iter;
	}

	public long countRows(final String tableName)
			throws TableNotFoundException {
		Iterator<Entry<Key, Value>> iter = this.getTableIterator(tableName);
		long counter = 0;
		while (iter.hasNext()) {
			iter.next();
			++counter;
		}
		return counter;
	}

	public long readAllData(final String tableName)
			throws TableNotFoundException {
		Iterator<Entry<Key, Value>> iter = this.getTableIterator(tableName);
		long counter = 0;
		while (iter.hasNext()) {
			++counter;
			Entry<Key, Value> e = iter.next();
			Text colf = e.getKey().getColumnFamily();
			Text colq = e.getKey().getColumnQualifier();
			System.out.print("row: " + e.getKey().getRow() + ", colf: " + colf
					+ ", colq: " + colq);
			System.out.println(", value: " + e.getValue().toString());
		}
		return counter;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

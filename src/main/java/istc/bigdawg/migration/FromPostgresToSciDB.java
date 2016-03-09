/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;

/**
 * Migrate data from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 *
 */
public class FromPostgresToSciDB
		implements FromDatabaseToDatabase, MigrationNetworkRequest {

	/**
	 * the object of the class is serializable
	 */
	private static final long serialVersionUID = 1L;

	private PostgreSQLConnectionInfo connectionFrom;
	private String fromTable;
	private SciDBConnectionInfo connectionTo;
	private String toArray;

	/* log */
	private static Logger log = Logger.getLogger(FromPostgresToSciDB.class);

	/**
	 * This is migration from PostgreSQL to SciDB.
	 * 
	 * @param connectionFrom the connection to PostgreSQL @param fromTable the
	 * name of the table in PostgreSQL to be migrated @param connectionTo the
	 * connection to SciDB database @param arrayTo the name of the array in
	 * SciDB
	 * 
	 * @see
	 * istc.bigdawg.migration.FromDatabaseToDatabase#migrate(istc.bigdawg.query.
	 * ConnectionInfo, java.lang.String, istc.bigdawg.query.ConnectionInfo,
	 * java.lang.String
	 */
	@Override
	public MigrationResult migrate(ConnectionInfo connectionFrom,
			String fromTable, ConnectionInfo connectionTo, String toArray)
					throws MigrationException {
		log.debug("General data migration: " + this.getClass().getName());
		if (connectionFrom instanceof PostgreSQLConnectionInfo
				&& connectionTo instanceof SciDBConnectionInfo) {
			try {
				this.connectionFrom = (PostgreSQLConnectionInfo) connectionFrom;
				this.fromTable = fromTable;
				this.connectionTo = (SciDBConnectionInfo) connectionTo;
				this.toArray = toArray;
				/*
				 * check if the address is not a local host
				 */
				String hostname = connectionTo.getHost();
				log.debug("SciDB hostname: " + hostname);
				if (!hostname.equals("localhost")
						|| !hostname.equals("127.0.0.1")) {
					InetAddress address = InetAddress.getByName(hostname);
					/*
					 * the following method getLocalHost() might not be the
					 * right one to use, the machine can have many network
					 * interfaces
					 */
					InetAddress localAddress = InetAddress.getLocalHost();
					/*
					 * if SciDB is not on this server then send the message to
					 * the server where the SciDB instance resides
					 */
					log.debug("localAddress: " + localAddress + " ip address: "
							+ localAddress.getHostAddress());
					log.debug("scidb address (address): " + address + " ip address: " + address.getHostAddress());
					if (!address.getHostAddress()
							.equals(localAddress.getHostAddress())) {
						log.debug("Migration will be executed remotely.");
						Object result = NetworkOut.send(this, hostname);
						if (result instanceof MigrationResult) {
							return (MigrationResult) result;
						}
						if (result instanceof MigrationException) {
							throw (MigrationException) result;
						}
						if (result instanceof NetworkException) {
							NetworkException ex = (NetworkException) result;
							log.error("Problem on the other host: "
									+ ex.getMessage());
							throw new MigrationException(
									"Problem on the other host: "
											+ ex.getMessage());
						}
						String message = "Migration was executed on a remote host but the returned result is unexepcted: "
								+ result.toString();
						log.error(message);
						throw new MigrationException(message);
					}
				}
				/* execute the migration locally */
				log.debug("Migration will be executed locally.");
				return execute();
			} catch (MigrationException | UnknownHostException
					| NetworkException e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	/**
	 * Execute the migration
	 * 
	 * @return
	 * @throws MigrationException
	 */
	public MigrationResult execute() throws MigrationException {
		if (this.connectionFrom == null || this.fromTable == null
				|| this.connectionTo == null || this.toArray == null) {
			throw new MigrationException("The object was not initialized");
		}
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(
				connectionFrom, fromTable, connectionTo, toArray);
		return migrator.migrate();
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws MigrationException
	 */
	public static void main(String[] args)
			throws MigrationException, IOException {
		LoggerSetup.setLogging();
		PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "tpch", "postgres", "test");
		String fromTable = "region";
		SciDBConnectionInfo conTo = new SciDBConnectionInfo("localhost", "1239",
				"scidb", "mypassw", "/opt/scidb/14.12/bin/");
		String toArray = "region2";
		FromPostgresToSciDB migrator = new FromPostgresToSciDB();
		migrator.migrate(conFrom, fromTable, conTo, toArray);
	}

}

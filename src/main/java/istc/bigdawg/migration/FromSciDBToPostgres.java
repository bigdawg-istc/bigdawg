/**
 * 
 */
package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.net.InetAddress;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;

/**
 * Migrate data from SciDB to PostgreSQL.
 * 
 * @author Adam Dziedzic
 */
public class FromSciDBToPostgres
		implements FromDatabaseToDatabase, MigrationNetworkRequest {

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 1L;
	/* log */
	private static Logger log = Logger.getLogger(FromSciDBToPostgres.class);

	private SciDBConnectionInfo connectionFrom;
	private String fromArray;
	private PostgreSQLConnectionInfo connectionTo;
	private String toTable;

	@Override
	/**
	 * Migrate data from SciDB to PostgreSQL.
	 */
	public MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException {
		log.debug("General data migration: " + this.getClass().getName());
		if (connectionFrom instanceof SciDBConnectionInfo
				&& connectionTo instanceof PostgreSQLConnectionInfo) {
			this.connectionFrom = (SciDBConnectionInfo) connectionFrom;
			this.fromArray = objectFrom;
			this.connectionTo = (PostgreSQLConnectionInfo) connectionTo;
			this.toTable = objectTo;
			try {
				/*
				 * check if the address is not a local host
				 */
				String hostname = connectionFrom.getHost();
				log.debug("SciDB hostname: " + hostname);
				if (!isThisMyIpAddress(InetAddress.getByName(hostname))) {
					log.debug("Migration will be executed remotely.");
					Object result = NetworkOut.send(this, hostname);
					return processResult(result);
				}
				/* execute the migration locally */
				log.debug("Migration will be executed locally.");
				return execute();
			} catch (Exception e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.MigrationNetworkRequest#execute()
	 */
	@Override
	public MigrationResult execute() throws MigrationException {
		if (this.connectionFrom == null || this.fromArray == null
				|| this.connectionTo == null || this.toTable == null) {
			throw new MigrationException("The object was not initialized");
		}
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(
				connectionFrom, fromArray, connectionTo, toTable);
		return migrator.migrate();
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		LoggerSetup.setLogging();
		FromSciDBToPostgres migrator = new FromSciDBToPostgres();
		SciDBConnectionInfo conFrom = new SciDBConnectionInfo("localhost",
				"1239", "scidb", "mypassw", "/opt/scidb/14.12/bin/");
		String arrayFrom = "region2";
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "postgres", "test");
		String tableTo = "region";
		MigrationResult result = migrator.migrate(conFrom, arrayFrom, conTo, tableTo);
		System.out.println(result);
	}

}

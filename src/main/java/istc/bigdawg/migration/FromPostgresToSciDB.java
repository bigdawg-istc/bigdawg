/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;

/**
 * Migrate data from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 *
 */
public class FromPostgresToSciDB extends FromDatabaseToDatabase {

	/* log */
	private static Logger log = Logger.getLogger(FromPostgresToSciDB.class);

	/**
	 * The object of the class is serializable.
	 */
	private static final long serialVersionUID = 1L;

	private PostgreSQLConnectionInfo connectionFrom;
	private String fromTable;
	private SciDBConnectionInfo connectionTo;
	private String toArray;

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
				return this.dispatch();
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.FromDatabaseToDatabase#getConnectionFrom()
	 */
	@Override
	public ConnectionInfo getConnectionFrom() {
		return connectionFrom;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.FromDatabaseToDatabase#getConnecitonTo()
	 */
	@Override
	public ConnectionInfo getConnecitonTo() {
		return connectionTo;
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
				"localhost", "5431", "test", "postgres", "test");
		// PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo(
		// "localhost", "5431", "test", "postgres", "test");
		String fromTable = "region";
		SciDBConnectionInfo conTo = new SciDBConnectionInfo("localhost", "1239",
				"scidb", "mypassw", "/opt/scidb/14.12/bin/");
		String toArray = "region";
		FromPostgresToSciDB migrator = new FromPostgresToSciDB();
		MigrationResult result = migrator.migrate(conFrom, fromTable, conTo,
				toArray);
		System.out.println("migration result: " + result);
	}

}

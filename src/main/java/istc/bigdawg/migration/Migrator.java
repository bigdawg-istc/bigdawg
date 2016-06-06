
/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;

/**
 * The main interface to the migrator module.
 * 
 * @author Adam Dziedzic
 */
public class Migrator {

	private static Logger logger = Logger.getLogger(Migrator.class.getName());
	private static List<FromDatabaseToDatabase> registeredMigrators;

	/**
	 * Types of data migration, for example, CSV_PARALLEL means that data are
	 * moved in CSV format in many threads
	 * 
	 * @author Adam Dziedzic
	 * 
	 *         Feb 25, 2016 5:25:05 PM
	 */
	public enum Type {
		CSV_PARALLEL, BINARY_PARALLEL, CSV_SEQUENTIAL, BINARY_SEQUENTIAL
	}

	/**
	 * register the migrators
	 */
	static {
		registeredMigrators = new ArrayList<FromDatabaseToDatabase>();
		registeredMigrators.add(new FromPostgresToPostgres());
		registeredMigrators.add(new FromPostgresToSciDB());
		registeredMigrators.add(new FromSciDBToPostgres());
	}

	/**
	 * Migrate data between databases.
	 * 
	 * @param connectionFrom
	 *            the connection to the database from which we migrate the data.
	 * @param objectFrom
	 *            the array/table from which we migrate the data
	 * @param connectionTo
	 *            the connection to the database to which we migrate the data
	 * @param objectTo
	 *            the array/table to which we migrate the data
	 * @return the result and information about the executed migration
	 * 
	 * @throws MigrationException
	 *             information why the migration failed (e.g. no access to one
	 *             of the database, schemas are not compatible
	 */
	public static MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException {
		logger.debug("Migrator - main facade.");
		for (FromDatabaseToDatabase migrator : registeredMigrators) {
			MigrationResult result = migrator.migrate(connectionFrom,
					objectFrom, connectionTo, objectTo);
			if (result != null) {
				return result;
			}
		}
		throw new MigrationException("Unsupported migration from "
				+ connectionFrom.getHost() + ":" + connectionFrom.getPort()
				+ " to " + connectionTo.getHost() + ":" + connectionTo.getPort()
				+ "!\n" + "Detail info:\n" + "From:\n"
				+ connectionFrom.toString() + "\n To:\n"
				+ connectionTo.toString());
	}

	public static void main(String[] args) {
		LoggerSetup.setLogging();
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "mimic2", "pguser", "test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				"localhost", "5430", "mimic2_copy", "pguser", "test");
		ConnectionInfo conFrom = conInfoFrom;
		ConnectionInfo conTo = conInfoTo;
		MigrationResult result;
		try {
			result = Migrator.migrate(conFrom, "mimic2v26.d_patients", conTo,
					"mimic2v26.d_patients");
			logger.debug("Number of extracted rows: "
					+ result.getCountExtractedElements()
					+ " Number of loaded rows: "
					+ result.getCountLoadedElements());
		} catch (MigrationException e) {
			String msg = "Problem with general data Migrator.";
			logger.error(msg);
			e.printStackTrace();
		}
	}

}

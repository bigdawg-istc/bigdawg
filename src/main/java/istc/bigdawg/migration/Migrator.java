
/**
 * 
 */
package istc.bigdawg.migration;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.direct.FromPostgresToPostgres;
import istc.bigdawg.migration.direct.FromPostgresToSciDB;
import istc.bigdawg.migration.direct.FromSciDBToPostgres;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;

/**
 * The main interface to the migrator module.
 * 
 * @author Adam Dziedzic
 */
public class Migrator {

	/**
	 * log
	 */
	private static Logger logger = Logger.getLogger(Migrator.class.getName());

	/**
	 * Register migrators - they will be called for migration between database
	 * when the requests come.
	 */
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
	 * see:
	 * {@link #migrate(ConnectionInfo, String, ConnectionInfo, String, MigrationParams)}
	 * 
	 * @param connectionFrom
	 * @param objectFrom
	 * @param connectionTo
	 * @param objectTo
	 * @return {@link MigrationResult} the result and information about the
	 *         executed migration
	 * @throws MigrationException
	 */
	public static MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException {
		return Migrator.migrate(connectionFrom, objectFrom, connectionTo,
				objectTo, null);
	}

	/**
	 * Migrate data between databases.
	 * 
	 * @param connectionFrom
	 *            the connection to the database from which we migrate the data
	 * @param objectFrom
	 *            the array/table from which we migrate the data
	 * @param connectionTo
	 *            the connection to the database to which we migrate the data
	 * @param objectTo
	 *            the array/table to which we migrate the data
	 * @param migrationParams
	 *            additional parameters for the migrator, for example, the
	 *            "create statement" (a statement to create an object:
	 *            table/array) which should be executed in the database
	 *            identified by connectionTo; data should be loaded to this new
	 *            object, the name of the target object in the create statement
	 *            has to be the same as the migrate method parameter: objectTo
	 * @return {@link MigrationResult} the result and information about the
	 *         executed migration
	 * 
	 * @throws MigrationException
	 *             information why the migration failed (e.g. no access to one
	 *             of the database, schemas are not compatible
	 */
	public static MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo,
			MigrationParams migrationParams) throws MigrationException {
		logger.debug("Migrator - main facade.");
		for (FromDatabaseToDatabase migrator : registeredMigrators) {
			MigrationResult result = migrator
					.migrate(new MigrationInfo(connectionFrom, objectFrom,
							connectionTo, objectTo, migrationParams));
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


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
 * @author Adam Dziedzic
 * 
 *
 */
public class Migrator {

	private static Logger logger = Logger.getLogger(Migrator.class.getName());
	private static List<FromDatabaseToDatabase> registeredMigrators;
	
	public enum Type {
		CSV_PARALLEL, BINARY_PARALLEL, CSV_SEQUENTIAL, BINARY_SEQUENTIAL
	}

	static {
		registeredMigrators = new ArrayList<FromDatabaseToDatabase>();
		registeredMigrators.add(new FromPostgresToPostgres());
		registeredMigrators.add(new FromPostgresToSciDB());
		registeredMigrators.add(new FromSciDBToPostgres());
	}

	public static MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
			String objectTo) throws MigrationException {
		logger.debug("Migrator - main facade.");
		for (FromDatabaseToDatabase migrator : registeredMigrators) {
			MigrationResult result = migrator.migrate(connectionFrom, objectFrom, connectionTo, objectTo);
			if (result != null) {
				return result;
			}
		}
		throw new MigrationException("Unsupported migration from " + connectionFrom.getHost() + ":"
				+ connectionFrom.getPort() + " to " + connectionTo.getHost() + ":" + connectionTo.getPort() + "!\n"
				+ "Detail info:\n" + "From:\n" + connectionFrom.toString() + "\n To:\n" + connectionTo.toString());
	}

	public static void main(String[] args) {
		try {
			LoggerSetup.setLogging();
		} catch (IOException e1) {
			e1.printStackTrace();
			System.err.print("Logger setup failed!");
		}
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo("localhost", "5431", "mimic2", "pguser",
				"test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo("localhost", "5430", "mimic2_copy", "pguser",
				"test");
		ConnectionInfo conFrom = conInfoFrom;
		ConnectionInfo conTo = conInfoTo;
		MigrationResult result;
		try {
			result = Migrator.migrate(conFrom, "mimic2v26.d_patients", conTo, "mimic2v26.d_patients");
			logger.debug("Number of extracted rows: " + result.getCountExtractedElements() + " Number of loaded rows: "
					+ result.getCountLoadedElements());
		} catch (MigrationException e) {
			String msg = "Problem with general data Migrator.";
			logger.error(msg);
			e.printStackTrace();
		}
	}

}

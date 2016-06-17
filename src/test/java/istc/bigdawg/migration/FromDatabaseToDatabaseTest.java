/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromDatabaseToDatabaseTest {
	/**
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromDatabaseToDatabaseTest.class.getName());

	private PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private String fromTable = "region";
	private SciDBConnectionInfo conTo = new SciDBConnectionInfo();
	// private SciDBConnectionInfo conTo = new
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String toArray = "region";
	private long numberOfRowsPostgres;

	@Test
	public void testExecuteMigrationLocally()
			throws MigrationException, SQLException, IOException {
		MigrationInfo migrationInfo = new MigrationInfo(conFrom, fromTable,
				conTo, toArray);
		FromDatabaseToDatabase migrator = new FromDatabaseToDatabase(
				ExportPostgres.ofFormat(FileFormat.CSV),
				LoadSciDB.ofFormat(FileFormat.CSV), migrationInfo);

		/*
		 * Prepare the test data in a table in PostgreSQL.
		 * 
		 * Prepare the source table and delete the target array.
		 */
		this.numberOfRowsPostgres = TestMigrationUtils
				.loadDataToPostgresRegionTPCH(conFrom, fromTable);
		SciDBHandler.dropArrayIfExists(conTo, toArray);

		MigrationResult result = migrator.executeMigrationLocally();
		logger.debug("Result of data migration: " + result.toString());

		TestMigrationUtils.checkNumberOfElements(conTo, toArray,
				numberOfRowsPostgres);
		// drop the created array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LoggerSetup.setLogging();
		logger.debug("Test for general migration.");

	}

}

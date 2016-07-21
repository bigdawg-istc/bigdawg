/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Utils;

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

	private PostgreSQLConnectionInfo conPostgres = new PostgreSQLConnectionInfoTest();
	private String table = "region";
	private SciDBConnectionInfo conSciDB = new SciDBConnectionInfo();
	// private SciDBConnectionInfo conTo = new
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String array = "region";
	private long numberOfRowsPostgres;
	private long numberOfCellsSciDB;

	@Before
	public void beforeTests() {
		LoggerSetup.setLogging();
	}

	@Test
	public void testExecuteMigrationLocallyFromPostgresToSciDB()
			throws MigrationException, SQLException, IOException {
		MigrationInfo migrationInfo = new MigrationInfo(conPostgres, table,
				conSciDB, array, null);
		FromDatabaseToDatabase migrator = new FromDatabaseToDatabase(
				ExportPostgres.ofFormat(FileFormat.CSV),
				LoadSciDB.ofFormat(FileFormat.CSV), migrationInfo);

		/*
		 * Prepare the test data in a table in PostgreSQL.
		 * 
		 * Prepare the source table and delete the target array.
		 */
		this.numberOfRowsPostgres = TestMigrationUtils
				.loadDataToPostgresRegionTPCH(conPostgres, table);
		SciDBHandler.dropArrayIfExists(conSciDB, array);
		TestMigrationUtils.prepareFlatTargetArray(conSciDB, array);

		MigrationResult result = migrator.executeMigrationLocally();
		logger.debug("Result of data migration: " + result.toString());

		TestMigrationUtils.checkNumberOfElementsSciDB(conSciDB, array,
				numberOfRowsPostgres);
		// drop the created array
		SciDBHandler.dropArrayIfExists(conSciDB, array);
	}

	@Test
	public void testExecuteMigrationLocallyFromSciDBToPostgres()
			throws SQLException, IOException, MigrationException {
		MigrationInfo migrationInfo = new MigrationInfo(conSciDB, array,
				conPostgres, table, null);
		FromDatabaseToDatabase migrator = new FromDatabaseToDatabase(
				ExportSciDB.ofFormat(FileFormat.CSV),
				LoadPostgres.ofFormat(FileFormat.CSV), migrationInfo);
		String flatArray = array + "_flat_";
		numberOfCellsSciDB = TestMigrationUtils.loadRegionDataToSciDB(conSciDB,
				flatArray, array);
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conPostgres);
		handler.dropTableIfExists(table);
		MigrationResult migrationResult = migrator.executeMigrationLocally();
		logger.debug("Migration result: " + migrationResult);
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conPostgres,
				table);
		assertEquals(numberOfCellsSciDB, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(table);
	}

	@Test
	public void testMigrationLocalRemoteFromPostgresToSciDB()
			throws MigrationException, SQLException, IOException {
		MigratorTask migratorTask = null;
		try {
			migratorTask = new MigratorTask();
			MigrationInfo migrationInfo = new MigrationInfo(conPostgres, table,
					conSciDB, array);
			logger.debug("Migration info: " + migrationInfo.toString());
			FromDatabaseToDatabase migrator = new FromDatabaseToDatabase(
					ExportPostgres.ofFormat(FileFormat.CSV),
					LoadSciDB.ofFormat(FileFormat.CSV), migrationInfo);
			/*
			 * Prepare the test data in a table in PostgreSQL and in an array in
			 * SciDB.
			 */
			this.numberOfRowsPostgres = TestMigrationUtils
					.loadDataToPostgresRegionTPCH(conPostgres, table);

			TestMigrationUtils.prepareFlatTargetArray(conSciDB, array);

			MigrationResult result = migrator.executeMigrationLocalRemote();
			logger.debug("Result of data migration: " + result.toString());

			TestMigrationUtils.checkNumberOfElementsSciDB(conSciDB, array,
					numberOfRowsPostgres);
		} finally {
			// drop the created array
			SciDBHandler.dropArrayIfExists(conSciDB, array);
			if (migratorTask != null) {
				migratorTask.close();
			}
		}
	}

	@Test
	public void testMigrationLocalRemoteFromSciDBToPostgres() throws Exception {
		MigratorTask migratorTask = null;
		try {
			migratorTask = new MigratorTask();
			MigrationInfo migrationInfo = new MigrationInfo(conSciDB, array,
					conPostgres, table);
			logger.debug("Migration info: " + migrationInfo.toString());
			FromDatabaseToDatabase migrator = new FromDatabaseToDatabase(
					ExportSciDB.ofFormat(FileFormat.CSV),
					LoadPostgres.ofFormat(FileFormat.CSV), migrationInfo);

			String flatArray = array + "_flat_";
			numberOfCellsSciDB = TestMigrationUtils
					.loadRegionDataToSciDB(conSciDB, flatArray, array);
			// make sure that the target array does not exist
			PostgreSQLHandler handler = new PostgreSQLHandler(conPostgres);
			handler.dropTableIfExists(table);
			handler.close();
			MigrationResult result = migrator.executeMigrationLocalRemote();
			logger.debug("Result of data migration: " + result.toString());
			long postgresCountTuples = Utils
					.getPostgreSQLCountTuples(conPostgres, table);
			assertEquals(numberOfCellsSciDB, postgresCountTuples);
			// drop the created table
			handler = new PostgreSQLHandler(conPostgres);
			handler.dropTableIfExists(table);
			handler.close();
		} finally {
			if (migratorTask != null) {
				migratorTask.close();
			}
		}
	}

	@After
	public void after() {
		System.out.println(
				"The end of the " + this.getClass().getName() + " test!");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LoggerSetup.setLogging();
		logger.debug("Test for general migration.");

	}

}

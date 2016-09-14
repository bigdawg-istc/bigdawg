/**
 * 
 */
package istc.bigdawg.migration;

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
import istc.bigdawg.scidb.SciDBConnectionInfoTest;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * Test the migration from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToSciDBRegionTPCHDataTest {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToSciDBRegionTPCHDataTest.class);

	private FromDatabaseToDatabase migrator;
	private PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private String fromTable = "region_test_from_13241";
	private SciDBConnectionInfo conTo = new SciDBConnectionInfoTest();
	// private SciDBConnectionInfo conTo = new
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String toArray = "region_test_to_13241";
	private long numberOfRowsPostgres = 0;

	@Before
	/**
	 * Prepare the test data in a table in PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void beforeLoadDataToPostgres() throws SQLException, IOException {
		LoggerSetup.setLogging();
		this.numberOfRowsPostgres = TestMigrationUtils
				.loadDataToPostgresRegionTPCH(conFrom, fromTable);
		/* this.migrator = = new FromPostgresToSciDB(); */
		this.migrator = new FromDatabaseToDatabase(
				ExportPostgres.ofFormat(FileFormat.CSV),
				LoadSciDB.ofFormat(FileFormat.CSV));
	}

	@Test
	/**
	 * If the test fails, first check if the target array is already in the
	 * SciDB database.
	 * 
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public void testFromPostgresToSciDBPreparedArray()
			throws SQLException, MigrationException {
		TestMigrationUtils.prepareFlatTargetArray(conTo, toArray);
		/*
		 * test of the main method
		 */
		migrator.migrate(new MigrationInfo(conFrom, fromTable, conTo, toArray));
		TestMigrationUtils.checkNumberOfElementsSciDB(conTo, toArray,
				numberOfRowsPostgres);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void testGeneralMigrationWithParams() throws Exception {
		logger.debug("General migration from Postgres to Postgres "
				+ "with a given parameter: craete target table");

		MigrationParams migrationParams = new MigrationParams(
				TestMigrationUtils.getCreateFlatArrayForRegion(toArray));
		/* drop the array if exists before the test */
		SciDBHandler.dropArrayIfExists(conTo, toArray);

		Migrator.migrate(conFrom, fromTable, conTo, toArray, migrationParams);
		TestMigrationUtils.checkNumberOfElementsSciDB(conTo, toArray,
				numberOfRowsPostgres);
		/* drop the created array */
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	/**
	 * Check if a target array is created when only the target array name is
	 * given.
	 * 
	 * @throws MigrationException
	 * @throws SQLException
	 */
	public void testFromPostgresToSciDBNoTargetArray()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		migrator.migrate(new MigrationInfo(conFrom, fromTable, conTo, toArray));
		TestMigrationUtils.checkNumberOfElementsSciDB(conTo, toArray,
				numberOfRowsPostgres);
		// drop the created array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	/**
	 * The data should be loaded to the multi-dimensional target array and after
	 * the process the intermediate flat array should be removed.
	 * 
	 * @throws MigrationException
	 * @throws SQLException
	 */
	public void testFromPostgresToSciDBMultiDimensionalTargetArray()
			throws MigrationException, SQLException {
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement(TestMigrationUtils
				.getCreateMultiDimensionalArrayForRegion(toArray));
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		migrator.migrate(new MigrationInfo(conFrom, fromTable, conTo, toArray));
		TestMigrationUtils.checkNumberOfElementsSciDB(conTo, toArray,
				numberOfRowsPostgres);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@After
	/**
	 * Remove the test table for PostgreSQL.
	 * 
	 * @throws SQLException
	 */
	public void afterRemovePostgreSQLTestTable() throws SQLException {
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
	}

}

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
import istc.bigdawg.scidb.SciDBConnectionInfoTest;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Utils;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 7:09:13 PM
 */
public class FromSciDBToPostgresTest {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToPostgresTest.class);

	private FromSciDBToPostgres migrator = new FromSciDBToPostgres();
	private PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfoTest();
	private String toTable = "region_test_from_13255_";
	private SciDBConnectionInfo conFrom = new SciDBConnectionInfoTest();
	private String fromArray = "region_test_from_13255_";
	private String flatArray;
	private long numberOfCellsSciDB = 0L;

	@Before
	/**
	 * Prepare the test data in an array in SciDB.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void beforeTests() throws SQLException, IOException {
		LoggerSetup.setLogging();
		flatArray = fromArray + "_flat_";
		numberOfCellsSciDB = TestMigrationUtils.loadRegionDataToSciDB(conFrom,
				flatArray, fromArray);
	}

	@Test
	public void testFromSciDBToPostgreSQLNoTargetTable()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		migrator.migrate(new MigrationInfo(conFrom, fromArray, conTo, toTable));
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDB, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
	}

	@Test
	public void testFromSciDBToPostgreSQLWithPreparedTargetTable()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable(
				TestMigrationUtils.getCreateRegionTableStatement(toTable));
		migrator.migrate(new MigrationInfo(conFrom, fromArray, conTo, toTable));
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDB, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println(
				"The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBToPostgresWithParameters()
			throws SQLException, MigrationException {
		logger.debug("General migration from Postgres to Postgres "
				+ "with a given parameter: craete target table");

		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		MigrationParams migrationParams = new MigrationParams(
				TestMigrationUtils.getCreateRegionTableStatement(toTable));
		/* We migrate from the multi-dimensional array so drop to flat one. */
		SciDBHandler.dropArrayIfExists(conFrom, flatArray);

		Migrator.migrate(conFrom, fromArray, conTo, toTable, migrationParams);
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDB, postgresCountTuples);
	}

	@After
	public void after() {
		System.out.println(
				"The end of the " + this.getClass().getName() + " test!");
	}

}

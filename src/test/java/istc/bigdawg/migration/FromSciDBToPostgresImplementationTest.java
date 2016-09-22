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
import istc.bigdawg.utils.Utils;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 7:09:13 PM
 */
public class FromSciDBToPostgresImplementationTest {

	/* log */
	private static Logger log = Logger
			.getLogger(FromSciDBToPostgresImplementationTest.class);

	private PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfoTest();
	private String toTable = "region_test_from_13255___to_postgres_bin";
	private SciDBConnectionInfo conFrom = new SciDBConnectionInfo();
	private String flatArray = "region_test_from_13255__from_scidb_bin";

	/* multi-dimensional array */
	private String multiDimArray = "region_test_from_13255__from_scidb_dim_bin";

	/** Number of cells in the flat array. */
	private long numberOfCellsSciDBFlat = 0L;

	/** Number of cells in the multi-dimensional array. */
	private long numberOfCellsSciDBMultiDim = 0L;

	@Before
	/**
	 * Prepare the test data in a table in SciDB.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void loadDataToSciDB() throws SQLException, IOException {
		LoggerSetup.setLogging();
		long numberOfCellsSciDB = TestMigrationUtils
				.loadRegionDataToSciDB(conFrom, flatArray, multiDimArray);
		numberOfCellsSciDBFlat = numberOfCellsSciDB;
		numberOfCellsSciDBMultiDim = numberOfCellsSciDB;
	}

	@Test
	public void testFromSciDBmulitDimensionalArrayToPostgreSQLNoTargetTable()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conFrom, multiDimArray, conTo, toTable));
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDBMultiDim, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
	}

	@Test
	public void testFromSciDBMutliDimensionalArrayToPostgreSQLWithPreparedTargetTableBin()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable("create table " + toTable
				+ " (r_regionkey BIGINT NOT NULL, r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conFrom, multiDimArray, conTo, toTable));
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDBMultiDim, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println(
				"The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBToPostgreSQLNoTargetTable()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conFrom, flatArray, conTo, toTable));
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDBFlat, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
	}

	@Test
	public void testFromSciDBFlatArrayToPostgreSQLWithPreparedTargetTableBin()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable("create table " + toTable
				+ " (r_regionkey BIGINT NOT NULL, r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conFrom, flatArray, conTo, toTable));
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDBFlat, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println(
				"The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBMutliDimensionalArrayToPostgreSQLWithPreparedTargetTableBinMigrateNoDimensionsFromSciDBArray()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable("create table " + toTable
				+ " (r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conFrom, multiDimArray, conTo, toTable));
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDBMultiDim, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println(
				"The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBMutliDimensionalArrayToPostgreSQLWithPreparedTargetTableCSVMigrateNoDimensionsFromSciDBArray()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable(
				TestMigrationUtils.getCreateRegionTableStatement(toTable));
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conFrom, multiDimArray, conTo, toTable));
		migrator.migrateSingleThreadCSV();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDBMultiDim, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println(
				"The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBMutliDimensionalArrayToPostgreSQLWithPreparedTargetTableCSVFullMigrationWithDimensionsAndAttributes()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable("create table " + toTable
				+ " (r_regionkey BIGINT NOT NULL, r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conFrom, multiDimArray, conTo, toTable));
		migrator.migrateSingleThreadCSV();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDBMultiDim, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println(
				"The end of the test with prepared table in PosgreSQL.");
	}

	@After
	public void after() {
		log.debug("The end of the " + this.getClass().getCanonicalName()
				+ " test!");
	}

}

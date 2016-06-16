/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.direct.FromSciDBToPostgresImplementation;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Utils;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 7:09:13 PM
 */
public class FromSciDBToPostgresImplementationTest {

	/* log */
	private static Logger log = Logger.getLogger(FromSciDBToPostgresImplementationTest.class);

	private PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfoTest();
	private String toTable = "region_test_from_13255___to_postgres_bin";
	private SciDBConnectionInfo conFrom = new SciDBConnectionInfo();
	private String flatArray = "region_test_from_13255__from_scidb_bin";
	/* mutli dimensional array */
	private String multiDimArray = "region_test_from_13255__from_scidb_dim_bin";
	private long numberOfCellsSciDBFlat = 0;
	private long numberOfCellsSciDBmulti = 0;

	@Before
	/**
	 * Prepare the test data in a table in SciDB.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void loadDataToSciDB() throws SQLException, IOException {
		LoggerSetup.setLogging();
		log.info("load data to SciDB");
		SciDBHandler handler = new SciDBHandler(conFrom);
		SciDBHandler.dropArrayIfExists(conFrom, flatArray);
		SciDBHandler.dropArrayIfExists(conFrom, flatArray);
		handler.executeStatementAFL(
				"create array " + flatArray + "<r_regionkey:int64,r_name:string,r_comment:string> [i=0:*,1000000,0]");
		handler.close();

		handler = new SciDBHandler(conFrom);
		File file = new File("src/test/resources/region.scidb");
		String absolutePath = file.getAbsolutePath();
		String loadCommand = "load(" + flatArray + ", '" + absolutePath + "')";
		handler.executeStatementAFL(loadCommand);
		handler.commit();
		handler.close();

		numberOfCellsSciDBFlat = Utils.getNumberOfCellsSciDB(conFrom, flatArray);
		assertEquals(5L, numberOfCellsSciDBFlat);

		// prepare the target array
		SciDBHandler.dropArrayIfExists(conFrom, multiDimArray);
		handler = new SciDBHandler(conFrom);
		handler.executeStatement(
				"create array " + multiDimArray + " <r_name:string,r_comment:string> [r_regionkey=0:*,1000000,0]");
		handler.commit();
		handler.close();

		handler = new SciDBHandler(conFrom);
		String command = "store(redimension(" + flatArray + "," + multiDimArray + ")," + multiDimArray + ")";
		log.debug(command);
		handler.executeStatementAFL(command);
		handler.commit();
		handler.close();

		numberOfCellsSciDBmulti = Utils.getNumberOfCellsSciDB(conFrom, multiDimArray);
		assertEquals(5L, numberOfCellsSciDBmulti);
	}

	@Test
	public void testFromSciDBmulitDimensionalArrayToPostgreSQLNoTargetTable() throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, multiDimArray,
				conTo, toTable);
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDBmulti, postgresCountTuples);
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
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, multiDimArray,
				conTo, toTable);
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDBmulti, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println("The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBToPostgreSQLNoTargetTable() throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, flatArray, conTo,
				toTable);
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDBFlat, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
	}

	@Test
	public void testFromSciDBToPostgreSQLWithPreparedTargetTable() throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable("create table " + toTable
				+ " (r_regionkey BIGINT NOT NULL, r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, flatArray, conTo,
				toTable);
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDBFlat, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println("The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBMutliDimensionalArrayToPostgreSQLWithPreparedTargetTableBinMigrateNoDimensionsFromSciDBArray()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable(
				"create table " + toTable + " (r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, multiDimArray,
				conTo, toTable);
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDBmulti, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println("The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBMutliDimensionalArrayToPostgreSQLWithPreparedTargetTableCSVMigrateNoDimensionsFromSciDBArray()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable(
				"create table " + toTable + " (r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, multiDimArray,
				conTo, toTable);
		migrator.migrateSingleThreadCSV();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDBmulti, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println("The end of the test with prepared table in PosgreSQL.");
	}

	@Test
	public void testFromSciDBMutliDimensionalArrayToPostgreSQLWithPreparedTargetTableCSVFullMigrationWithDimensionsAndAttributes()
			throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		handler.createTable("create table " + toTable
				+ " (r_regionkey BIGINT NOT NULL, r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);");
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, multiDimArray,
				conTo, toTable);
		migrator.migrateSingleThreadCSV();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDBmulti, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println("The end of the test with prepared table in PosgreSQL.");
	}

	@After
	public void after() {
		System.out.println("The end of the " + this.getClass().getName() + " test!");
	}

}

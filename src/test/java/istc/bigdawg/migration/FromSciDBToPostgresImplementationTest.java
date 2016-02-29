/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

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

/**
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 7:09:13 PM
 */
public class FromSciDBToPostgresImplementationTest {

	private PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfoTest();
	private String toTable = "region_test_from_13255___to_postgres_bin";
	private SciDBConnectionInfo conFrom = new SciDBConnectionInfo();
	private String fromArray = "region_test_from_13255__from_scidb_bin";
	private long numberOfCellsSciDB = 0;

	@Before
	/**
	 * Prepare the test data in a table in SciDB.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void loadDataToSciDB() throws SQLException, IOException {
		LoggerSetup.setLogging();
		SciDBHandler handler = new SciDBHandler(conFrom);
		SciDBHandler.dropArrayIfExists(conFrom, fromArray);
		SciDBHandler.dropArrayIfExists(conFrom, fromArray);
		handler.executeStatementAFL(
				"create array " + fromArray + "<r_regionkey:int64,r_name:string,r_comment:string> [i=0:*,1000000,0]");
		handler.close();

		handler = new SciDBHandler(conFrom);
		File file = new File("src/test/resources/region.scidb");
		String absolutePath = file.getAbsolutePath();
		String loadCommand = "load(" + fromArray + ", '" + absolutePath + "')";
		handler.executeStatementAFL(loadCommand);
		handler.commit();
		handler.close();
		numberOfCellsSciDB = Utils.getNumberOfCellsSciDB(conFrom, fromArray);
		assertEquals(5L, numberOfCellsSciDB);
	}

	@Test
	public void testFromSciDBToPostgreSQLNoTargetTable() throws MigrationException, SQLException {
		// make sure that the target array does not exist
		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropTableIfExists(toTable);
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, fromArray, conTo,
				toTable);
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDB, postgresCountTuples);
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
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, fromArray, conTo,
				toTable);
		migrator.migrateBin();
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo, toTable);
		assertEquals(numberOfCellsSciDB, postgresCountTuples);
		// drop the created table
		handler.dropTableIfExists(toTable);
		System.out.println("The end of the test with prepared table in PosgreSQL.");
	}

	@After
	public void after() {
		System.out.println("The end of the " + this.getClass().getName() + " test!");
	}

}

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
import istc.bigdawg.utils.Utils;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class FromSciDBToPostgresNetworkTest {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromSciDBToPostgresNetworkTest.class);

	private FromSciDBToPostgres migrator = new FromSciDBToPostgres();
	// private PostgreSQLConnectionInfo conTo = new
	// PostgreSQLConnectionInfoTest();
	private PostgreSQLConnectionInfo conTo;
	private String toTable = "region_test_from_13255_";
	// private SciDBConnectionInfo conFrom = new SciDBConnectionInfoTest();
	private SciDBConnectionInfo conFrom;
	private String fromArray = "region_test_from_13255_";
	private String flatArray;
	private long numberOfCellsSciDB = 0L;

	private String localIP = "205.208.122.55";
	private String remoteIPmadison = "128.135.11.26";
	private String remoteIPfrancisco = "128.135.11.131";

	private String localPassword = "test";
	private String remotePassword = "ADAM12345testBorja2016";
	private String scidbPassword = "scidb123";
	private String scidbBinPath = "/opt/scidb/14.12/bin/";

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
		conFrom = new SciDBConnectionInfo(remoteIPmadison, "1239", "scidb",
				scidbPassword, scidbBinPath);
		conTo = new PostgreSQLConnectionInfo(remoteIPfrancisco, "5431", "test",
				"pguser", remotePassword);
		numberOfCellsSciDB = TestMigrationUtils.loadRegionDataToSciDB(conFrom,
				flatArray, fromArray);
	}

	@Test
	public void testFromSciDBToPostgreSQLNoTargetTableNetwork()
			throws MigrationException, SQLException {
		logger.debug("Migration from local SciDB to remote PostgreSQL.");
		// make sure that the target array does not exist

		PostgreSQLHandler handler = new PostgreSQLHandler(conTo);
		handler.dropDataSetIfExists(toTable);
		migrator.migrate(new MigrationInfo(conFrom, fromArray, conTo, toTable));
		long postgresCountTuples = Utils.getPostgreSQLCountTuples(conTo,
				toTable);
		assertEquals(numberOfCellsSciDB, postgresCountTuples);
		// drop the created table
		handler.dropDataSetIfExists(toTable);
	}

	@After
	public void after() {
		System.out.println(
				"The end of the " + this.getClass().getName() + " test!");
	}

}

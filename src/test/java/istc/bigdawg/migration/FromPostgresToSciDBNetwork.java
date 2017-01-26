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
 * Test migration from PostgreSQL to SciDB via network.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToSciDBNetwork {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToSciDBNetwork.class);

	private FromDatabaseToDatabase migrator;

	// private PostgreSQLConnectionInfo conFrom = new
	// PostgreSQLConnectionInfoTest();
	private PostgreSQLConnectionInfo conFrom;
	private String fromTable = "region_test_from_13241";
	// private SciDBConnectionInfo conTo = new SciDBConnectionInfoTest();
	private SciDBConnectionInfo conTo;
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String toArray = "region_test_to_13241";
	private long numberOfRowsPostgres = 0;

	private String localIP = "205.208.122.55";
	private String remoteIPmadison = "128.135.11.26";
	private String remoteIPfrancisco = "128.135.11.131";

	private String localPassword = "test";
	private String remotePassword = "ADAM12345testBorja2016";
	private String scidbPassword = "scidb123";
	private String scidbBinPath = "/opt/scidb/14.12/bin/";

	@Before
	/**
	 * Prepare the test data in a table in PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void beforeLoadDataToPostgres() throws SQLException, IOException {
		LoggerSetup.setLogging();
		this.conTo = new SciDBConnectionInfo(remoteIPfrancisco, "1239", "scidb",
				scidbPassword, scidbBinPath);
		this.conFrom = new PostgreSQLConnectionInfo(remoteIPmadison, "5431",
				"test", "pguser", remotePassword);
		this.numberOfRowsPostgres = TestMigrationUtils
				.loadDataToPostgresRegionTPCH(conFrom, fromTable)
				.getRowNumber();
		this.migrator = new FromPostgresToSciDB();
		/*
		 * this.migrator = new FromDatabaseToDatabase(
		 * ExportPostgres.ofFormat(FileFormat.CSV),
		 * LoadSciDB.ofFormat(FileFormat.CSV));
		 */
	}

	@Test
	/**
	 * If the test fails, first check if the target array is already in the
	 * SciDB database.
	 * 
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public void testFromPostgresToSciDBNetwork()
			throws SQLException, MigrationException {
		logger.debug("Test the migration from Postgres to SciDB via network.");
		TestMigrationUtils.prepareFlatTargetArray(conTo, toArray);
		/*
		 * test of the main method
		 */
		// Migrator.migrate(conFrom, fromTable, conTo, toArray);
		migrator = new FromPostgresToSciDB();
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

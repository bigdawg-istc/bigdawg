/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.direct.FromPostgresToSciDB;
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
 * 
 *         Feb 18, 2016 10:26:20 AM
 */
public class FromPostgresToSciDBTest {

	private FromPostgresToSciDB migrator = new FromPostgresToSciDB();
	private PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private String fromTable = "region_test_from_13241";
	private SciDBConnectionInfo conTo = new SciDBConnectionInfoTest();
	// private SciDBConnectionInfo conTo = new
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String toArray = "region_test_from_13241";
	private long numberOfRowsPostgres = 0;

	@Before
	/**
	 * Prepare the test data in a table in PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void loadDataToPostgres() throws SQLException, IOException {
		this.numberOfRowsPostgres = TestMigrationUtils
				.loadDataToPostgresRegionTPCH(conFrom, fromTable);
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
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray
				+ " <r_regionkey:int64,r_name:string,r_comment:string> [i=0:*,1000000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		migrator.migrate(conFrom, fromTable, conTo, toArray);
		TestMigrationUtils.checkNumberOfElements(conTo, toArray,
				numberOfRowsPostgres);
		// clean: remove the target array
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
		migrator.migrate(conFrom, fromTable, conTo, toArray);
		TestMigrationUtils.checkNumberOfElements(conTo, toArray,
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
		handler.executeStatement("create array " + toArray
				+ " <r_name:string,r_comment:string> [r_regionkey=0:*,1000000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		migrator.migrate(conFrom, fromTable, conTo, toArray);
		TestMigrationUtils.checkNumberOfElements(conTo, toArray,
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
	public void removePostgreSQLTestTable() throws SQLException {
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
	}

}

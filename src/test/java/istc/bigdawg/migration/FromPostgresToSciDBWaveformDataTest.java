/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Utils;

/**
 * Test the implementation of the migration from PostgreSQL to SciDB. It tests
 * both, csv and bin migrations.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToSciDBWaveformDataTest {

	/* log */
	private static Logger log = Logger
			.getLogger(FromPostgresToSciDBWaveformDataTest.class);

	private ConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private String fromTable = "waveform";
	private ConnectionInfo conTo = new SciDBConnectionInfo();
	// private SciDBConnectionInfo conTo = new
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String toArray = "waveform4";
	private long numberOfRowsPostgres = 0L;

	@Before
	/**
	 * Prepare the test data in a table in PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void loadDataToPostgres() throws SQLException, IOException {
		LoggerSetup.setLogging();
		this.numberOfRowsPostgres = TestMigrationUtils
				.loadDataToPostgresWaveform(conFrom, fromTable);
	}

	@Test
	public void testCsvFlatArrays()
			throws MigrationException, SQLException, IOException {
		log.info("csv test flat");
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);

		handler.executeStatement("create array " + toArray
				+ " <id:int64,time:int64,value:double> [i=0:*,1000000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDB migrator = new FromPostgresToSciDB(
				new MigrationInfo(conFrom, fromTable, conTo, toArray));
		migrator.migrateSingleThreadCSV();
		checkNumberOfElementsInSciDB(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void testCsvMultiDimensionalArrays()
			throws MigrationException, SQLException {
		log.info("csv test multi-dimensional");
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray
				+ " <value:double> [id=0:*,1000,0,time=0:*,1000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDB migrator = new FromPostgresToSciDB(
				new MigrationInfo(conFrom, fromTable, conTo, toArray));
		migrator.migrateSingleThreadCSV();
		checkNumberOfElementsInSciDB(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void testBinFlat() throws MigrationException, SQLException {
		log.info("bin test flat");
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray
				+ " <id:int64,time:int64,value:double> [i=0:*,1000000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDB migrator = new FromPostgresToSciDB(
				new MigrationInfo(conFrom, fromTable, conTo, toArray, null));
		migrator.migrateBin();
		checkNumberOfElementsInSciDB(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void testBinMulti() throws MigrationException, SQLException {
		log.info("bin test multi-dimensional");
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray
				+ " <value:double> [id=0:*,1000,0,time=0:*,1000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDB migrator = new FromPostgresToSciDB(
				new MigrationInfo(conFrom, fromTable, conTo, toArray, null));
		migrator.migrateBin();
		checkNumberOfElementsInSciDB(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	/**
	 * Verify the number of elements/cells in a SciDB array.
	 * 
	 * @param conTo
	 * @param toArray
	 * @throws SQLException
	 */
	private void checkNumberOfElementsInSciDB(ConnectionInfo conTo,
			String toArray) throws SQLException {
		long numberOfCellsSciDB = Utils.getNumberOfCellsSciDB(conTo, toArray);
		assertEquals(numberOfRowsPostgres, numberOfCellsSciDB);
	}

}

/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.direct.FromPostgresToSciDBImplementation;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Utils;

/**
 * Test the implementation of the migration from PostgreSQL to SciDB. It tests
 * both, csv and bin migrations.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToSciDBImplementationTest {

	/* log */
	private static Logger log = Logger
			.getLogger(FromPostgresToSciDBImplementationTest.class);

	private PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private String fromTable = "waveform";
	private SciDBConnectionInfo conTo = new SciDBConnectionInfo();
	// private SciDBConnectionInfo conTo = new
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String toArray = "waveform4";
	private long numberOfRowsPostgres = 0;

	@Before
	/**
	 * Prepare the test data in a table in PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void loadDataToPostgres() throws SQLException, IOException {
		LoggerSetup.setLogging();
		log.info("Preparing the PostgreSQL source table.");
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
		handler.createTable("CREATE TABLE " + fromTable
				+ " (id bigint not null,time bigint not null,value double precision not null)");
		Connection con = PostgreSQLHandler.getConnection(conFrom);
		con.setAutoCommit(false);
		CopyManager cpTo = new CopyManager((BaseConnection) con);
		// InputStream input =
		// FromPostgresToSciDBTest.class.getClassLoader().getResourceAsStream("/home/adam/data/waveform_1GB.csv");
		String userName = System.getProperty("user.name");
		FileInputStream input = new FileInputStream(
				new File("/home/" + userName + "/data/waveform_test.csv"));
		// CHECK IF THE INPUT STREAM CONTAINS THE REQUIRED DATA
		// int size = 384;
		// byte[] buffer = new byte[size];
		// input.read(buffer, 0, size);
		// String in = new String(buffer, StandardCharsets.UTF_8);
		// System.out.println(in);
		numberOfRowsPostgres = cpTo.copyIn(
				"Copy " + fromTable
						+ " from STDIN with (format csv, delimiter ',')",
				input);
		con.commit();
		con.close();
		assertEquals(10, numberOfRowsPostgres);
	}

	@Test
	public void csvTestFlat()
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
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(
				conFrom, fromTable, conTo, toArray);
		migrator.migrateSingleThreadCSV();
		checkNumberOfElementsInSciDB(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void csvTestMulti() throws MigrationException, SQLException {
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
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(
				conFrom, fromTable, conTo, toArray);
		migrator.migrateSingleThreadCSV();
		checkNumberOfElementsInSciDB(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void binTestFlat() throws MigrationException, SQLException {
		log.info("bin test flat");
		// prepare the target array
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray
				+ " <id:int64,time:int64,value:double> [i=0:*,1000000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(
				conFrom, fromTable, conTo, toArray);
		migrator.migrateBin();
		checkNumberOfElementsInSciDB(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void binTestMulti() throws MigrationException, SQLException {
		log.info("bin test multi-dimensional");
		// prepare the target array
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray
				+ " <value:double> [id=0:*,1000,0,time=0:*,1000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(
				conFrom, fromTable, conTo, toArray);
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
	private void checkNumberOfElementsInSciDB(SciDBConnectionInfo conTo,
			String toArray) throws SQLException {
		long numberOfCellsSciDB = Utils.getNumberOfCellsSciDB(conTo, toArray);
		assertEquals(numberOfRowsPostgres, numberOfCellsSciDB);
	}

}

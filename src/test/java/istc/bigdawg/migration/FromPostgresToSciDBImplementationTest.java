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

import org.junit.Before;
import org.junit.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

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
 *
 */
public class FromPostgresToSciDBImplementationTest {

	private PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private String fromTable = "waveform";
	private SciDBConnectionInfo conTo = new SciDBConnectionInfo();
	// private SciDBConnectionInfo conTo = new
	// SciDBConnectionInfo("localhost","1239", "scidb", "mypassw",
	// "/opt/scidb/14.12/bin/");
	// private String toArray =
	// "bigdawg_region_test_from_13241_FromPostgresToSciDBTest";
	private String toArray = "waveform";
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
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
		handler.createTable("CREATE TABLE " + fromTable
				+ " (id bigint not null,time bigint not null,value double precision not null)");
		Connection con = PostgreSQLHandler.getConnection(conFrom);
		con.setAutoCommit(false);
		CopyManager cpTo = new CopyManager((BaseConnection) con);
		// InputStream input =
		// FromPostgresToSciDBTest.class.getClassLoader().getResourceAsStream("/home/adam/data/waveform_1GB.csv");
		FileInputStream input = new FileInputStream(new File("/home/adam/data/waveform_test.csv"));
		// CHECK IF THE INPUT STREAM CONTAINS THE REQUIRED DATA
		// int size = 384;
		// byte[] buffer = new byte[size];
		// input.read(buffer, 0, size);
		// String in = new String(buffer, StandardCharsets.UTF_8);
		// System.out.println(in);
		numberOfRowsPostgres = cpTo.copyIn("Copy " + fromTable + " from STDIN with (format csv, delimiter ',')", input);
		con.commit();
		con.close();
		assertEquals(10, numberOfRowsPostgres);
	}

	@Test
	public void csvTestFlat() throws MigrationException, SQLException, IOException {
		LoggerSetup.setLogging();
		System.out.println("csv test flat");
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray + " <id:int64,time:int64,value:double> [i=0:*,1000000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(conFrom, fromTable, conTo,
				toArray);
		migrator.migrateSingleThreadCSV();
		checkNumberOfElements(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void csvTestMulti() throws MigrationException, SQLException {
		System.out.println("csv test multi-dimensional");
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray + " <value:double> [id=0:*,1000,0,time=0:*,1000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(conFrom, fromTable, conTo,
				toArray);
		migrator.migrateSingleThreadCSV();
		checkNumberOfElements(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void binTestFlat() throws MigrationException, SQLException {
		System.out.println("bin test flat");
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray + " <id:int64,time:int64,value:double> [i=0:*,1000000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(conFrom, fromTable, conTo,
				toArray);
		migrator.migrateBin();
		checkNumberOfElements(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	@Test
	public void binTestMulti() throws MigrationException, SQLException {
		System.out.println("bin test multi-dimensional");
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement("create array " + toArray + " <value:double> [id=0:*,1000,0,time=0:*,1000,0]");
		handler.commit();
		handler.close();
		/*
		 * test of the main method
		 */
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(conFrom, fromTable, conTo,
				toArray);
		migrator.migrateBin();
		checkNumberOfElements(conTo, toArray);
		// clean: remove the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
	}

	private void checkNumberOfElements(SciDBConnectionInfo conTo, String toArray) throws SQLException {
		long numberOfCellsSciDB = Utils.getNumberOfCellsSciDB(conTo, toArray);
		assertEquals(numberOfRowsPostgres, numberOfCellsSciDB);
	}

	@Test
	public void binTest() throws SQLException, IOException {
		System.out.println("bin test");
	}

}

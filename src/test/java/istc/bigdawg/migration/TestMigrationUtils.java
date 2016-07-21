/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Utils;

/**
 * Utilities to be used for testing migration (e.g. pre-load some data, check
 * number of elements in arrays/tables).
 * 
 * @author Adam Dziedzic
 */
public class TestMigrationUtils {

	/* log */
	private static Logger log = Logger.getLogger(TestMigrationUtils.class);

	/** Number of rows in the region table. */
	final static long REGION_ROWS_NUMBER = 5L;

	/** Number of rows in the waveform table. */
	final static long WAVEFORM_ROWS_NUMBER = 10L;

	/** Name for the table in TPCH. */
	private static final String REGION_TPCH_TABLE = "region";

	/** Name for the table from mimi2 - waveform. */
	private static final String WAVEFORM_MIMIC_TABLE = "waveform";

	/**
	 * SQL statement for creation of a table with a given name and structure as
	 * for the region table from the TPC-H benchmark.
	 * 
	 * @param tableName
	 *            the name of the table
	 * @return the SQL statement to create the table
	 */
	static String getCreateRegionTableStatement(String tableName) {
		String createTable = "CREATE TABLE " + tableName
				+ " (r_regionkey BIGINT NOT NULL," + "r_name CHAR(25) NOT NULL,"
				+ "r_comment VARCHAR(152) NOT NULL)";
		/* log.debug("Create table statement: " + createTable); */
		return createTable;
	}

	/**
	 * The statement (AFL/AQL) for SciDB to create a flat array which can store
	 * data from the region table from the TPC-H benchmark.
	 * 
	 * @param flatArrayName
	 *            the statement to create the flat array
	 * @return
	 */
	static String getCreateFlatArrayForRegion(String flatArrayName) {
		String flatArray = "create array " + flatArrayName
				+ "<r_regionkey:int64,r_name:string,r_comment:string> "
				+ "[i=0:*,1000000,0]";
		/* log.debug("Create flat array statement: " + flatArray); */
		return flatArray;
	}

	/**
	 * Get AFL/AQL statement for creation of a multidimensional array.
	 * 
	 * @param toArray
	 *            array name
	 * 
	 * @return create multi-dimensional array statement for SciDB (this array is
	 *         for data from the region table from the TPC-H benchmark).
	 */
	static String getCreateMultiDimensionalArrayForRegion(String toArray) {
		String createArray = "create array " + toArray
				+ " <r_name:string,r_comment:string> [r_regionkey=0:*,1000000,0]";
		log.debug("creat multi-dimensional array statement: " + createArray);
		return createArray;
	}

	/**
	 * Prepare a flat target array.
	 * 
	 * @param conTo
	 * @param toArray
	 * @throws SQLException
	 */
	static void prepareFlatTargetArray(ConnectionInfo conTo, String toArray)
			throws SQLException {
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement(getCreateFlatArrayForRegion(toArray));
		handler.commit();
		handler.close();
	}

	/**
	 * Prepare a multi-dimensional target array.
	 * 
	 * @param conTo
	 * @param toArray
	 * @throws SQLException
	 */
	static void prepareMultiDimensionalTargetArray(ConnectionInfo conTo,
			String toArray) throws SQLException {
		// prepare the target array
		SciDBHandler.dropArrayIfExists(conTo, toArray);
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatement(
				getCreateMultiDimensionalArrayForRegion(toArray));
		handler.commit();
		handler.close();
	}

	/**
	 * Load region TPC-H data to PostgreSQL.
	 * 
	 * @param conFrom
	 *            connection to PostgreSQL
	 * @param fromTable
	 *            the table to load the data to in PostgreSQL
	 * 
	 * @return number of loaded rows
	 * 
	 */
	public static long loadDataToPostgresRegionTPCH(
			PostgreSQLConnectionInfo conFrom, String fromTable)
					throws SQLException, IOException {
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
		handler.createTable(getCreateRegionTableStatement(fromTable));
		Connection con = PostgreSQLHandler.getConnection(conFrom);
		con.setAutoCommit(false);
		CopyManager cpTo = new CopyManager((BaseConnection) con);
		InputStream input = TestMigrationUtils.class.getClassLoader()
				.getResourceAsStream(REGION_TPCH_TABLE + ".csv");
		// FileInputStream input = new FileInputStream(new
		// File("./region.csv"));
		// CHECK IF THE INPUT STREAM CONTAINS THE REQUIRED DATA
		// int size = 384;
		// byte[] buffer = new byte[size];
		// input.read(buffer, 0, size);
		// String in = new String(buffer, StandardCharsets.UTF_8);
		// System.out.println(in);
		long numberOfRowsPostgres = cpTo.copyIn(
				"Copy " + fromTable
						+ " from STDIN with (format csv, delimiter '|')",
				input);
		con.commit();
		con.close();
		assertEquals(REGION_ROWS_NUMBER, numberOfRowsPostgres);
		return numberOfRowsPostgres;
	}

	/**
	 * Load waveform data to PostgreSQL.
	 * 
	 * @param conFrom
	 *            connection to PostgreSQL
	 * @param fromTable
	 *            the table to load the data to
	 * @return number of loaded rows
	 * @throws SQLException
	 * @throws IOException
	 */
	public static long loadDataToPostgresWaveform(ConnectionInfo conFrom,
			String fromTable) throws SQLException, IOException {
		log.info("Preparing the PostgreSQL source table.");
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
		handler.createTable("CREATE TABLE " + fromTable
				+ " (id bigint not null," + "time bigint not null,"
				+ "value double precision not null)");
		Connection con = PostgreSQLHandler.getConnection(conFrom);
		con.setAutoCommit(false);
		CopyManager cpTo = new CopyManager((BaseConnection) con);
		InputStream input = FromPostgresToSciDBRegionTPCHDataTest.class
				.getClassLoader()
				.getResourceAsStream(WAVEFORM_MIMIC_TABLE + ".csv");
		// CHECK IF THE INPUT STREAM CONTAINS THE REQUIRED DATA
		// int size = 384;
		// byte[] buffer = new byte[size];
		// input.read(buffer, 0, size);
		// String in = new String(buffer, StandardCharsets.UTF_8);
		// System.out.println(in);
		long numberOfRowsPostgres = cpTo.copyIn(
				"Copy " + fromTable
						+ " from STDIN with (format csv, delimiter ',')",
				input);
		con.commit();
		con.close();
		assertEquals(WAVEFORM_ROWS_NUMBER, numberOfRowsPostgres);
		return numberOfRowsPostgres;
	}

	/**
	 * Load to SciDB the region data from TPC-H.
	 * 
	 * @return number of loaded cells to SciDB
	 * 
	 * @throws SQLException
	 * @throws IOException
	 * 
	 * 
	 */
	public static long loadRegionDataToSciDB(ConnectionInfo conFrom,
			String flatArray, String multiDimArray)
					throws SQLException, IOException {
		log.info("Load data to SciDB.");

		SciDBHandler.dropArrayIfExists(conFrom, flatArray);
		SciDBHandler.dropArrayIfExists(conFrom, multiDimArray);

		SciDBHandler handler = new SciDBHandler(conFrom);
		handler.executeStatementAFL(getCreateFlatArrayForRegion(flatArray));
		handler.close();

		File source = new File("src/test/resources/region.scidb");
		File target = new File("/tmp/region.scidb");
		Files.copy(Paths.get(source.getAbsolutePath()),
				Paths.get(target.getAbsolutePath()),
				StandardCopyOption.REPLACE_EXISTING);
		/*
		 * SciDB has a problem with reading from files due to access
		 * permissions. Thus, we have to copy the input files to the /tmp/
		 * directory.
		 */
		String loadCommand = "load(" + flatArray + ", '"
				+ target.getAbsolutePath() + "')";
		log.debug("Load to SciDB command: " + loadCommand);
		
		handler = new SciDBHandler(conFrom);
		handler.executeStatementAFL(loadCommand);
		handler.commit();
		handler.close();

		long numberOfCellsSciDBFlat = Utils.getNumberOfCellsSciDB(conFrom,
				flatArray);
		assertEquals(REGION_ROWS_NUMBER, numberOfCellsSciDBFlat);

		// prepare the target array
		SciDBHandler.dropArrayIfExists(conFrom, multiDimArray);
		
		handler = new SciDBHandler(conFrom);
		handler.executeStatement("create array " + multiDimArray + " "
				+ "<r_name:string,r_comment:string> "
				+ "[r_regionkey=0:*,1000000,0]");
		handler.commit();
		handler.close();

		handler = new SciDBHandler(conFrom);
		String command = "store(redimension(" + flatArray + "," + multiDimArray
				+ ")," + multiDimArray + ")";
		log.debug(command);
		handler.executeStatementAFL(command);
		handler.commit();
		handler.close();

		long numberOfCellsSciDBMultiDim = Utils.getNumberOfCellsSciDB(conFrom,
				multiDimArray);
		assertEquals(REGION_ROWS_NUMBER, numberOfCellsSciDBMultiDim);
		return numberOfCellsSciDBMultiDim;
	}

	/**
	 * Check if the number of loaded elements is correct. This is an internal
	 * method.
	 * 
	 * @param conTo
	 * @param toArray
	 * @throws SQLException
	 */
	public static void checkNumberOfElementsSciDB(SciDBConnectionInfo conTo,
			String toArray, long expectedNumberOfCells) throws SQLException {
		long numberOfCellsSciDB = Utils.getNumberOfCellsSciDB(conTo, toArray);
		assertEquals(expectedNumberOfCells, numberOfCellsSciDB);
	}

}

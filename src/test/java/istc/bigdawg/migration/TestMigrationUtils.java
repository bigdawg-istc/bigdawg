/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.utils.Utils;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class TestMigrationUtils {

	/** Name for the table in TPCH. */
	private static final String REGION_TPCH_TABLE = "region";

	public static long loadDataToPostgresRegionTPCH(
			PostgreSQLConnectionInfo conFrom, String fromTable)
					throws SQLException, IOException {
		LoggerSetup.setLogging();
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
		handler.createTable("CREATE TABLE " + fromTable
				+ " (r_regionkey BIGINT NOT NULL," + "r_name CHAR(25) NOT NULL,"
				+ "r_comment VARCHAR(152) NOT NULL)");
		Connection con = PostgreSQLHandler.getConnection(conFrom);
		con.setAutoCommit(false);
		CopyManager cpTo = new CopyManager((BaseConnection) con);
		InputStream input = FromPostgresToSciDBTest.class.getClassLoader()
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
		assertEquals(5, numberOfRowsPostgres);
		return numberOfRowsPostgres;
	}

	/**
	 * Check if the number of loaded elements is correct. This is an internal
	 * method.
	 * 
	 * @param conTo
	 * @param toArray
	 * @throws SQLException
	 */
	public static void checkNumberOfElements(SciDBConnectionInfo conTo,
			String toArray, long expectedNumberOfCells) throws SQLException {
		long numberOfCellsSciDB = Utils.getNumberOfCellsSciDB(conTo, toArray);
		assertEquals(expectedNumberOfCells, numberOfCellsSciDB);
	}

}

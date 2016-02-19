/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.scidb.jdbc.IResultSetWrapper;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 18, 2016 10:26:20 AM
 */
public class FromPostgresToSciDBTest {

	private FromPostgresToSciDB migrator = new FromPostgresToSciDB();
	private PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private String fromTable = "region_test_from_13241";
	private SciDBConnectionInfo conTo = new SciDBConnectionInfo("localhost", "1239", "scidb", "mypassw",
			"/opt/scidb/14.12/bin/");
	private String toArray = "region";
	private long numberOfRowsPostgres = 0;

	@Before
	public void loadDataToPostgres() throws SQLException, IOException {
		PostgreSQLHandler handler = new PostgreSQLHandler(conFrom);
		handler.dropTableIfExists(fromTable);
		handler.createTable("CREATE TABLE " + fromTable
				+ " (r_regionkey BIGINT NOT NULL,r_name CHAR(25) NOT NULL,r_comment VARCHAR(152) NOT NULL)");
		Connection con = PostgreSQLHandler.getConnection(conFrom);
		con.setAutoCommit(false);
		CopyManager cpTo = new CopyManager((BaseConnection) con);
		InputStream input = FromPostgresToSciDBTest.class.getClassLoader().getResourceAsStream("region.csv");
		// FileInputStream input = new FileInputStream(new
		// File("./region.csv"));
		// CHECK IF THE INPUT STREAM CONTAINS THE REQUIRED DATA
		// int size = 384;
		// byte[] buffer = new byte[size];
		// input.read(buffer, 0, size);
		// String in = new String(buffer, StandardCharsets.UTF_8);
		// System.out.println(in);
		numberOfRowsPostgres = cpTo.copyIn("Copy " + fromTable + " from STDIN with (format csv, delimiter '|')", input);
		con.commit();
		con.close();
		assertEquals(5, numberOfRowsPostgres);
	}

	@Test
	public void testFromPostgresToSciDBcsvSingleThreadMigration() throws SQLException {
		migrator.migrateSingleThreadCSV(conFrom, fromTable, conTo, toArray);
		Connection con = SciDBHandler.getConnection(conTo);
		Statement query = con.createStatement();
		ResultSet resultSet = query.executeQuery("select * from " + toArray);
		ResultSetMetaData meta = resultSet.getMetaData();

		System.out.println("Source array name: " + meta.getTableName(0));
		System.out.println(meta.getColumnCount() + " columns:");

		IResultSetWrapper resWrapper = resultSet.unwrap(IResultSetWrapper.class);
		for (int i = 1; i <= meta.getColumnCount(); i++) {
			System.out.println(meta.getColumnName(i) + " - " + meta.getColumnTypeName(i) + " - is attribute:"
					+ resWrapper.isColumnAttribute(i));
		}

		long numberOfCellsSciDB = 0;
		while (resultSet.next()) {
			// System.out.println(resultSet.getString(3));
			// System.out.println(resultSet.getInt(1));
			++numberOfCellsSciDB;
		}
		resultSet.close();
		query.close();
		con.close();
		assertEquals(numberOfRowsPostgres, numberOfCellsSciDB);
	}
	
	

}

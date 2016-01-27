/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class FromPostgresToPostgresTest {
	
	@Before
	public void setUp() throws IOException {
		LoggerSetup.setLogging();
	}

	@Test
	public void testFromPostgresToPostgres() throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		FromPostgresToPostgres migrator = new FromPostgresToPostgres();
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo("localhost", "5431", "mimic2", "pguser",
				"test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo("localhost", "5430", "mimic2_copy", "pguser",
				"test");
		PostgreSQLHandler postgres1 = new PostgreSQLHandler(conInfoFrom);
		PostgreSQLHandler postgres2 = new PostgreSQLHandler(conInfoTo);
		String tableName = "test1_from_postgres_to_postgres_";
		try {
			int intValue = 14;
			double doubleValue = 1.2;
			String stringValue = "adamdziedzic";
			String createTable = "create table " + tableName + "(a int,b double precision,c varchar)";
			postgres1.executeStatementPostgreSQL(createTable);

			String insertInto = "insert into " + tableName + " values(" + intValue + "," + doubleValue + ",'"
					+ stringValue + "')";
			System.out.println(insertInto);
			postgres1.executeStatementPostgreSQL(insertInto);

			postgres2.executeStatementPostgreSQL(createTable);

			MigrationResult result = migrator.migrate(conInfoFrom, tableName, conInfoTo, tableName);

			assertEquals(result.getCountExtractedElements(), Long.valueOf(1L));
			assertEquals(result.getCountLoadedElements(), Long.valueOf(1L));

			QueryResult qresult = postgres2.executeQueryPostgreSQL("select * from " + tableName);
			List<List<String>> rows = qresult.getRows();
			List<String> row = rows.get(0);
			int currentInt = Integer.parseInt(row.get(0));
			System.out.println("int value: " + currentInt);
			assertEquals(intValue, currentInt);

			double currentDouble = Double.parseDouble(row.get(1));
			System.out.println("double value: " + currentDouble);
			assertEquals(doubleValue, currentDouble, 0);

			String currentString = row.get(2);
			System.out.println("string value: " + currentString);
			assertTrue(stringValue.equals(currentString));

		} catch (SQLException | IOException e) {
			String msg = "Problem with data migration.";
			System.err.print(msg);
			e.printStackTrace();
			throw e;
		} finally {
			postgres1.executeStatementPostgreSQL("drop table " + tableName);
			postgres2.executeStatementPostgreSQL("drop table " + tableName);
		}

	}

}

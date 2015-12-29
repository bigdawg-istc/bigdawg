/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

import istc.bigdawg.migration.FromPostgresToPostgres.MigrationResult;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class FromPostgresToPostgresTest {

	@Test
	public void testFromPostgresToPostgres() {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		FromPostgresToPostgres migrator = new FromPostgresToPostgres();
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo("localhost", "5431", "mimic2", "pguser",
				"test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo("localhost", "5430", "mimic2_copy", "pguser",
				"test");
		PostgreSQLHandler postgres1 = new PostgreSQLHandler(conInfoFrom);
		PostgreSQLHandler postgres2 = new PostgreSQLHandler(conInfoTo);
		String tableName="test1_from_postgres_to_postgres_";
		try {
			int intValue = 14;
			double doubleValue = 1.2;
			String stringValue = "adamdziedzic";
			String createTable = "create table "+tableName+"(a int,b double precision,c varchar)";
			postgres1.executeNotQueryPostgreSQL(createTable);
			postgres1.executeNotQueryPostgreSQL(
					"insert into test1 values(" + intValue + "," + doubleValue + ",'" + stringValue + "')");

			postgres2.executeNotQueryPostgreSQL(createTable);

			MigrationResult result = migrator.migrate(conInfoFrom, "test1", conInfoTo, "test1");

			assertEquals(result.getCountExtractedRows(), Long.valueOf(1L));
			assertEquals(result.getCountLoadedRows(), Long.valueOf(1L));
			QueryResult qresult = postgres2.executeQueryPostgreSQL("select * from test1;");
			List<List<String>> rows = qresult.getRows();
			List<String> row = rows.get(0);
			assertEquals(row.get(0), intValue);
			assertEquals(row.get(1), doubleValue);
			assertEquals(row.get(2), stringValue);
			
		} catch (SQLException | IOException e) {
			String msg = "Problem with data migration.";
			System.err.print(msg);
			e.printStackTrace();
			try {
				postgres1.executeNotQueryPostgreSQL("drop table "+tableName);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			try {
				postgres2.executeNotQueryPostgreSQL("drop table "+tableName);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}

}

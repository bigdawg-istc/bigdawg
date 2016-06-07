/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import istc.bigdawg.executor.JdbcQueryResult;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Test data migration between Postgres instances.
 */
public class FromPostgresToPostgresTest {

	/* the class for the PostgreSQL <-> PostgreSQL migration */
	private FromPostgresToPostgres migrator = new FromPostgresToPostgres();

	private String localIP = "205.208.123.154";
	private String remoteIP = "128.135.11.26";

	private String localPassword = "test";
	private String remotePassword = "ADAM12345testBorja2016";

	@Before
	public void setUp() throws IOException {
		LoggerSetup.setLogging();
	}

	public void migrateTest(PostgreSQLConnectionInfo conInfoFrom,
			PostgreSQLConnectionInfo conInfoTo)
					throws MigrationException, SQLException {
		PostgreSQLHandler postgres1 = new PostgreSQLHandler(conInfoFrom);
		PostgreSQLHandler postgres2 = new PostgreSQLHandler(conInfoTo);
		String tableName = "test1_from_postgres_to_postgres_";
		try {
			int intValue = 14;
			double doubleValue = 1.2;
			String stringValue = "adamdziedzic";
			String createTable = "create table " + tableName
					+ "(a int,b double precision,c varchar)";
			postgres1.executeStatementPostgreSQL(createTable);

			String insertInto = "insert into " + tableName + " values("
					+ intValue + "," + doubleValue + ",'" + stringValue + "')";
			System.out.println(insertInto);
			postgres1.executeStatementPostgreSQL(insertInto);

			postgres2.executeStatementPostgreSQL(createTable);

			MigrationResult result = migrator.migrate(conInfoFrom, tableName,
					conInfoTo, tableName);

			assertEquals(Long.valueOf(1L), result.getCountExtractedElements());
			assertEquals(Long.valueOf(1L), result.getCountLoadedElements());

			JdbcQueryResult qresult = postgres2
					.executeQueryPostgreSQL("select * from " + tableName);
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

		} catch (SQLException e) {
			String msg = "Problem with data migration.";
			System.err.print(msg);
			e.printStackTrace();
			throw e;
		} finally {
			postgres1.executeStatementPostgreSQL("drop table " + tableName);
			postgres2.executeStatementPostgreSQL("drop table " + tableName);
		}
	}

	@Test
	public void testFromPostgresToPostgres() throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "mimic2", "pguser", localPassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				"localhost", "5430", "mimic2_copy", "pguser", localPassword);
		migrateTest(conInfoFrom, conInfoTo);
	}

	@Test
	public void testFromPostgresToPostgresNetworkFromLocalToRemote()
			throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				localIP, "5431", "test", "pguser", localPassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				remoteIP, "5431", "test", "pguser", remotePassword);
		migrateTest(conInfoFrom, conInfoTo);
	}

	@Test
	public void testFromPostgresToPostgresNetworkFromRemoteToLocal()
			throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				remoteIP, "5431", "test", "pguser", remotePassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				localIP, "5431", "test", "pguser", localPassword);
		migrateTest(conInfoFrom, conInfoTo);
	}

	@Test
	public void testFromPostgresToPostgresNetworkTPCH() throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				localIP, "5431", "tpch", "pguser", localPassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				remoteIP, "5431", "tpch", "pguser", remotePassword);
		String table = "lineitem";
		MigrationResult result = migrator.migrate(conInfoFrom, table, conInfoTo,
				table);
		System.out.println(result);
	}

	@Test
	public void testFromPostgresToPostgresNetworkTPCHRemote()
			throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				remoteIP, "5431", "tpch", "pguser", remotePassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				localIP, "5431", "tpch", "pguser", localPassword);
		String table = "supplier";
		MigrationResult result = migrator.migrate(conInfoFrom, table, conInfoTo,
				table);
		System.out.println(result);
	}

}

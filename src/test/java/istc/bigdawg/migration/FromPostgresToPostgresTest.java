/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.migration.direct.FromPostgresToPostgres;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         Test data migration between Postgres instances.
 */
public class FromPostgresToPostgresTest {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToPostgresTest.class);

	/** Data migrator for the PostgreSQL <-> PostgreSQL migration */
	private FromPostgresToPostgres migrator = new FromPostgresToPostgres();

	private String localIP = "205.208.122.55";
	private String remoteIPmadison = "128.135.11.26";
	private String remoteIPfrancisco = "128.135.11.131";

	private String localPassword = "test";
	private String remotePassword = "ADAM12345testBorja2016";

	/** The name of the source table in PostgreSQL. */
	private String tableNameFrom = "test1_from_postgres_to_postgres_table_from";

	/** The name of the target table in PostgreSQL. */
	private String tableNameTo = "test1_from_postgres_to_postgres_table_to";

	/** Dummy data for migration. */
	private int intValue = 14;
	private double doubleValue = 1.2;
	private String stringValue = "adamdziedzic";

	@Before
	public void setUp() throws IOException {
		LoggerSetup.setLogging();
	}

	/**
	 * 
	 * @param tableName
	 *            The table name in the database.
	 * @return SQL create table statement with the provided tableName.
	 */
	private String getCreateTableTest(String tableName) {
		String createTableSQL = "create table " + tableName
				+ "(a int,b double precision,c varchar)";
		logger.debug("create table SQL statement: " + createTableSQL);
		return createTableSQL;
	}

	/**
	 * 
	 * @param tableName The name of the table where the data should be inserted.
	 * @return SQL insert into statement for the test table.
	 */
	private String getInsertInto(String tableName) {
		String insertIntoSQL = "insert into " + tableNameFrom + " values("
				+ intValue + "," + doubleValue + ",'" + stringValue + "')";
		logger.debug("insert into test table SQL statement: " + insertIntoSQL);
		return insertIntoSQL;
	}

	private void migrateTest(PostgreSQLConnectionInfo conInfoFrom,
			PostgreSQLConnectionInfo conInfoTo)
					throws MigrationException, SQLException {
		PostgreSQLHandler postgres1 = new PostgreSQLHandler(conInfoFrom);
		PostgreSQLHandler postgres2 = new PostgreSQLHandler(conInfoTo);

		try {
			postgres1.executeStatementPostgreSQL(
					getCreateTableTest(tableNameFrom));

			postgres1.executeStatementPostgreSQL(getInsertInto(tableNameFrom));

			MigrationResult result = migrator.migrate(conInfoFrom,
					tableNameFrom, conInfoTo, tableNameTo);

			assertEquals(Long.valueOf(1L), result.getCountExtractedElements());
			assertEquals(Long.valueOf(1L), result.getCountLoadedElements());

			JdbcQueryResult qresult = postgres2
					.executeQueryPostgreSQL("select * from " + tableNameTo);
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
			logger.error(e.getMessage() + msg + " "
					+ StackTrace.getFullStackTrace(e), e);
			throw e;
		} finally {
			postgres1.executeStatementPostgreSQL(
					"drop table if exists " + tableNameFrom);
			postgres2.executeStatementPostgreSQL(
					"drop table if exists " + tableNameTo);
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
				remoteIPmadison, "5431", "test", "pguser", remotePassword);
		migrateTest(conInfoFrom, conInfoTo);
	}

	@Test
	public void testFromPostgresToPostgresNetworkFromRemoteToLocal()
			throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				remoteIPmadison, "5431", "test", "pguser", remotePassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				localIP, "5431", "test", "pguser", localPassword);
		migrateTest(conInfoFrom, conInfoTo);
	}

	@Test
	public void testFromPostgresToPostgresNetworkTPCH() throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				remoteIPmadison, "5431", "tpch", "pguser", remotePassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				remoteIPfrancisco, "5431", "tpch", "pguser", remotePassword);
		String table = "lineitem";
		MigrationResult result = migrator.migrate(conInfoFrom, table, conInfoTo,
				table);
		System.out.println(result);
	}

	@Test
	public void testFromPostgresToPostgresNetworkTPCHRemote() throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				remoteIPmadison, "5431", "tpch", "pguser", remotePassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				localIP, "5431", "tpch", "pguser", localPassword);
		String table = "supplier";
		MigrationResult result = migrator.migrate(conInfoFrom, table, conInfoTo,
				table);
		System.out.println(result);
	}

	/**
	 * 
	 * @param conFrom
	 * @param tableFrom
	 * @param conTo
	 * @param tableTo
	 * @return
	 */
	private Callable<MigrationResult> getMigrationTask(ConnectionInfo conFrom,
			String tableFrom, ConnectionInfo conTo, String tableTo) {
		Callable<MigrationResult> task = () -> {
			MigrationResult result = migrator.migrate(conFrom, tableFrom, conTo,
					tableTo);
			return result;
		};
		return task;
	}

	@Test
	public void testFromPostgresToPostgresNetworkLocking() throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		String secondRemoteIP = "128.135.11.131";
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				remoteIPmadison, "5431", "tpch", "pguser", remotePassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				secondRemoteIP, "5431", "tpch", "pguser", remotePassword);
		String table = "supplier";
		ExecutorService executor = Executors.newFixedThreadPool(2);
		Future<MigrationResult> migration1 = executor
				.submit(getMigrationTask(conInfoFrom, table, conInfoTo, table));
		MigrationResult result1 = migration1.get();
		/* reverse the migration direction */
		Future<MigrationResult> migration2 = executor
				.submit(getMigrationTask(conInfoTo, table, conInfoFrom, table));
		MigrationResult result2 = migration2.get();

		System.out.println("result of the first migration: " + result1);
		System.out.println("result of the second migration: " + result2);
	}

	@Test
	public void testFromPostgresToPostgresNetworkLocking3Machines()
			throws Exception {
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		String secondRemoteIP = "128.135.11.131";
		PostgreSQLConnectionInfo conInfoFrom1 = new PostgreSQLConnectionInfo(
				remoteIPmadison, "5431", "tpch", "pguser", remotePassword);
		PostgreSQLConnectionInfo conInfoFrom2 = new PostgreSQLConnectionInfo(
				localIP, "5431", "tpch", "pguser", "test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				secondRemoteIP, "5431", "tpch", "pguser", remotePassword);
		String table1 = "lineitem";
		String table2 = "part";
		ExecutorService executor = Executors.newFixedThreadPool(2);
		Future<MigrationResult> migration1 = executor.submit(
				getMigrationTask(conInfoFrom1, table1, conInfoTo, table1));
		TimeUnit.SECONDS.sleep(1);
		Future<MigrationResult> migration2 = executor.submit(
				getMigrationTask(conInfoFrom2, table2, conInfoTo, table2));
		MigrationResult result1 = migration1.get();
		MigrationResult result2 = migration2.get();

		System.out.println("result of the first migration: " + result1);
		System.out.println("result of the second migration: " + result2);
	}

}

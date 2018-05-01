/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
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
	protected FromPostgresToPostgres migrator = new FromPostgresToPostgres();

	protected String localIP = "205.208.122.55";
	protected String remoteIPmadison = "128.135.11.26";
	protected String remoteIPfrancisco = "128.135.11.131";

	protected String localPassword = "test";
	protected String remotePassword = "ADAM12345testBorja2016";

	/** The name of the source table in PostgreSQL. */
	protected String tableNameFrom = "test1_from_postgres_to_postgres_table_from";

	/** The name of the target table in PostgreSQL. */
	protected String tableNameTo = "test1_from_postgres_to_postgres_table_to";

	/** Additional parameters for the migration process. */
	protected MigrationParams migrationParams = null;

	/** Dummy data for migration. */
	protected int intValue = 14;
	protected double doubleValue = 1.2;
	protected String stringValue = "adamdziedzic";

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
	protected String getCreateTableTest(String tableName) {
		String createTableSQL = "create table " + tableName
				+ "(a int,b double precision,c varchar)";
		logger.debug("create table SQL statement: " + createTableSQL);
		return createTableSQL;
	}

	/**
	 * 
	 * @param tableName
	 *            The name of the table where the data should be inserted.
	 * @return SQL insert into statement for the test table.
	 */
	protected String getInsertInto(String tableName) {
		String insertIntoSQL = "insert into " + tableNameFrom + " values("
				+ intValue + "," + doubleValue + ",'" + stringValue + "')";
		logger.debug("insert into test table SQL statement: " + insertIntoSQL);
		return insertIntoSQL;
	}

	protected void migrateTest(PostgreSQLConnectionInfo conInfoFrom,
			PostgreSQLConnectionInfo conInfoTo)
					throws MigrationException, SQLException {
		PostgreSQLHandler postgres1 = new PostgreSQLHandler(conInfoFrom);
		PostgreSQLHandler postgres2 = new PostgreSQLHandler(conInfoTo);

		try {
			postgres1.executeStatementOnConnection(
					getCreateTableTest(tableNameFrom));

			postgres1.executeStatementOnConnection(getInsertInto(tableNameFrom));

			MigrationResult result = migrator
					.migrate(new MigrationInfo(conInfoFrom, tableNameFrom,
							conInfoTo, tableNameTo, migrationParams));

			assertEquals(Long.valueOf(1L), result.getCountExtractedElements());
			assertEquals(Long.valueOf(1L), result.getCountLoadedElements());

			JdbcQueryResult qresult = postgres2
					.executeQueryOnEngine("select * from " + tableNameTo);
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
			postgres1.executeStatementOnConnection(
					"drop table if exists " + tableNameFrom);
			postgres2.executeStatementOnConnection(
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
	public void testGeneralMigrationWithParams() throws Exception {
		logger.debug("General migration from Postgres to Postgres "
				+ "with a given parameter: craete target table");
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "pguser", localPassword);
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				"localhost", "5430", "test", "pguser", localPassword);
		migrationParams = new MigrationParams(getCreateTableTest(tableNameTo));
		migrateTest(conInfoFrom, conInfoTo);
	}

}

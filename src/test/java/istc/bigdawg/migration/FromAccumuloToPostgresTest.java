/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;
import org.jfree.util.Log;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.accumulo.AccumuloConnectionInfo;
import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.utils.Utils;

/**
 * Test the data migration from Accumulo to Postgres.
 * 
 * @author Adam Dziedzic
 */
public class FromAccumuloToPostgresTest {

	private static Logger logger = Logger
			.getLogger(FromAccumuloToPostgresTest.class);

	private static final String TABLE = "table_test_accumulo_postgres";
	private static final long COUNT_TUPLES_ACCUMULO = 2;
	private static final long COUNT_TUPLES_POSTGRES = 1;
	private static final String CREATE_TABLE;

	static {
		CREATE_TABLE = "create table " + TABLE + " (a varchar, b varchar);";
	}

	/**
	 * 
	 */
	public FromAccumuloToPostgresTest() {
		LoggerSetup.setLogging();
	}

	@Test
	public void fromAccumuloToPostgres() throws Exception {
		// TableOperations tabOp = null;
		// boolean tableAccumuloCreated = false;
		Connection con = null; /* For PostgreSQL. */
		try {
			AccumuloConnectionInfo connectionFrom = AccumuloInstance
					.getDefaultConnection();
			AccumuloInstance acc = AccumuloInstance
					.getFullInstance(connectionFrom);
			long countAccumulo = acc.countRows(TABLE);
			assertEquals(COUNT_TUPLES_ACCUMULO, countAccumulo);

			// tableAccumuloCreated = true;
			FromAccumuloToPostgres fromAccumuloToPostgres = null;
			PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfoTest();
			try {
				con = PostgreSQLHandler.getConnection(conInfoTo);
				con.setReadOnly(false);
				con.setAutoCommit(false);
			} catch (SQLException e1) {
				logger.error("Cannot create connection to PostgreSQL.");
				e1.printStackTrace();
				return;
			}
			try {
				PostgreSQLHandler.executeStatement(con,
						"drop table if exists " + TABLE);
				PostgreSQLHandler.createTargetTableSchema(con, TABLE,
						CREATE_TABLE);
			} catch (SQLException e1) {
				logger.error("Could not create table in PostgreSQL.");
				e1.printStackTrace();
				System.exit(1);
			}
			fromAccumuloToPostgres = new FromAccumuloToPostgres(acc, con);
			try {
				fromAccumuloToPostgres.fromAccumuloToPostgres(TABLE, TABLE);
			} catch (SQLException | TableNotFoundException | IOException e) {
				logger.error(
						"Could not migrate data from Accumulo to Postgres.");
				e.printStackTrace();
				System.exit(1);
			}
			logger.debug("The data was migrated from Accumulo to PostgreSQL.");
			try {
				long count = Utils.getPostgreSQLCountTuples(
						(PostgreSQLConnectionInfo) conInfoTo, TABLE);
				logger.debug("Number of tuples in PostgreSQL: " + count);
				assertEquals(COUNT_TUPLES_POSTGRES, count);
			} catch (SQLException e) {
				logger.error("Could not get count of tuples from PostgreSQL");
				e.printStackTrace();
			}

		} catch (AccumuloException | AccumuloSecurityException
				| AccumuloBigDawgException e) {
			Log.error("Could not create instance of fromAccumuloToPostgres");
			e.printStackTrace();
		} finally {
			// try {
			// if (tabOp != null && tableAccumuloCreated) {
			// tabOp.delete(TABLE);
			// }
			// try {
			// if (con != null) {
			// PostgreSQLHandler.executeStatement(con,
			// "drop table if exists " + TABLE);
			// }
			// } catch (SQLException e) {
			// logger.error("Could not remove table from PostgreSQL.");
			// e.printStackTrace();
			// }
			// } catch (AccumuloException | AccumuloSecurityException
			// | TableNotFoundException e) {
			// logger.error("Cannot delete table " + TABLE + " in accumulo.");
			// e.printStackTrace();
			//
			// }
		}

	}

	@Test
	public void fromAccumuloToPostgresMigrator() throws Exception {
		PostgreSQLConnectionInfo connectionTo = new PostgreSQLConnectionInfoTest();
		AccumuloConnectionInfo connectionFrom = AccumuloInstance
				.getDefaultConnection();
		logger.debug("Accumulo connection: " + connectionFrom.toString());
		AccumuloInstance acc = AccumuloInstance.getFullInstance(connectionFrom);
		long countAccumulo = acc.countRows(TABLE);
		assertEquals(COUNT_TUPLES_ACCUMULO, countAccumulo);
		Connection con;
		try {
			con = PostgreSQLHandler.getConnection(connectionTo);
			con.setReadOnly(false);
			con.setAutoCommit(false);
		} catch (SQLException e1) {
			logger.error("Cannot create connection to PostgreSQL.");
			e1.printStackTrace();
			return;
		}
		try {
			PostgreSQLHandler.executeStatement(con,
					"drop table if exists " + TABLE);
			PostgreSQLHandler.createTargetTableSchema(con, TABLE, CREATE_TABLE);
			con.commit();
			con.close();
		} catch (SQLException e1) {
			logger.error("Could not create table in PostgreSQL.");
			e1.printStackTrace();
			System.exit(1);
		}
		Migrator.migrate(connectionFrom, TABLE, connectionTo, TABLE);
		try {
			long count = Utils.getPostgreSQLCountTuples(
					(PostgreSQLConnectionInfo) connectionTo, TABLE);
			logger.debug("Number of tuples in PostgreSQL: " + count);
			assertEquals(COUNT_TUPLES_POSTGRES, count);
		} catch (SQLException e) {
			logger.error("Could not get count of tuples from PostgreSQL");
			e.printStackTrace();
		}
	}

	@Test
	public void fromAccumuloToPostgresWithParamsMigrator() throws Exception {
		PostgreSQLConnectionInfo connectionTo = new PostgreSQLConnectionInfoTest();
		AccumuloConnectionInfo connectionFrom = AccumuloInstance
				.getDefaultConnection();
		String tableFrom = TABLE + "new_to_migrate";
		String tableTo = TABLE + "new_from_migration";
		AccumuloInstance acc = AccumuloInstance.getFullInstance(connectionFrom);
		AccumuloTest.loadData(acc.getConn(), tableFrom);
		long countAccumulo = acc.countRows(tableFrom);
		long expectedTuples = 1;
		assertEquals(expectedTuples, countAccumulo);
		PostgreSQLHandler postgresHandler = new PostgreSQLHandler(connectionTo);
		postgresHandler
				.executeStatementPostgreSQL("drop table if exists " + tableTo);
		MigrationParams params = new MigrationParams("create table " + tableTo
				+ " (" + AccumuloTest.COL_QUAL + " varchar)");
		Migrator.migrate(connectionFrom, tableFrom, connectionTo, tableTo,
				params);
		try {
			long count = Utils.getPostgreSQLCountTuples(
					(PostgreSQLConnectionInfo) connectionTo, tableTo);
			logger.debug("Number of tuples in PostgreSQL: " + count);
			assertEquals(expectedTuples, count);
		} catch (SQLException e) {
			logger.error("Could not get count of tuples from PostgreSQL");
			e.printStackTrace();
		}
	}

}

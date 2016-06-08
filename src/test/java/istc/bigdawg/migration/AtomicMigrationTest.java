/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class AtomicMigrationTest {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(AtomicMigrationTest.class);

	private PostgreSQLConnectionInfo connectionFrom;
	private PostgreSQLConnectionInfo connectionTo;

	@Before
	public void setUp() {
		LoggerSetup.setLogging();
		connectionFrom = new PostgreSQLConnectionInfo("localhost", "5431",
				"tpch", "pguser", "test");
		connectionTo = new PostgreSQLConnectionInfo("localhost", "5430", "tpch",
				"pguser", "test");
	}

	private void prepareTable(String table) throws LocalQueryExecutionException {
		PostgreSQLHandler handlerFrom = new PostgreSQLHandler(connectionFrom);
		try {
			handlerFrom.execute(String.format(AtomicMigration.DROP_IF_EXISTS, table));
		} catch (LocalQueryExecutionException e) {
			logger.debug(e.getMessage());
			handlerFrom.execute(String.format(AtomicMigration.DROP_IF_EXISTS, table));
		}
		handlerFrom.execute("create table " + table + " as select * from "
				+ table + "_backup");

		PostgreSQLHandler handlerTo = new PostgreSQLHandler(connectionTo);
		handlerTo.execute(String.format(AtomicMigration.DROP_IF_EXISTS, table));
	}

	@Test
	public void testTableNoUpdatesDeletesInserts() throws SQLException,
			MigrationException, LocalQueryExecutionException {
		String table = "nation";
		prepareTable(table);
		AtomicMigration atomicMigration = new AtomicMigration(connectionFrom,
				table, connectionTo, table);
		atomicMigration.migrate();
	}

}

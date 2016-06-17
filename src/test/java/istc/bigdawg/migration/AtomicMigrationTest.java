/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.executor.JdbcQueryResult;
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

	private PostgreSQLHandler handlerFrom;
	private PostgreSQLHandler handlerTo;

	private static final String VALUES = "1,155190,7706,1,17,21168.23,0.04,0.02,"
			+ "'N','O','1996-03-13','1996-02-12','1996-03-22',"
			+ "'IN PERSON','TRUCK','egular courts above the'";

	@Before
	public void setUp() {
		LoggerSetup.setLogging();
		connectionFrom = new PostgreSQLConnectionInfo("localhost", "5431",
				"tpch", "pguser", "test");
		connectionTo = new PostgreSQLConnectionInfo("localhost", "5430", "tpch",
				"pguser", "test");
	}

	private void prepareTable(String table)
			throws LocalQueryExecutionException {
		handlerFrom = new PostgreSQLHandler(connectionFrom);
		try {
			handlerFrom.execute(
					String.format(AtomicMigration.DROP_IF_EXISTS, table));
		} catch (Exception e) {
			logger.debug(e.getMessage());
			handlerFrom.execute(String.format("drop view if exists " + table));
		}
		handlerFrom.execute("create table " + table + " as select * from "
				+ table + "_backup");

		handlerTo = new PostgreSQLHandler(connectionTo);
		handlerTo.execute(String.format(AtomicMigration.DROP_IF_EXISTS, table));
	}

	private int getRowNumber(PostgreSQLHandler handler, String table)
			throws SQLException {
		JdbcQueryResult result = handler
				.executeQueryPostgreSQL("select count(*) from " + table);
		int rowCount = Integer.valueOf(result.getRows().get(0).get(0));
		logger.debug("row count: " + rowCount);
		return rowCount;
	}

	@Test
	public void testTableNoUpdatesDeletesInserts() throws SQLException,
			MigrationException, LocalQueryExecutionException {
		String table = "nation";
		prepareTable(table);
		int sourceRowCount = getRowNumber(handlerFrom, table);
		AtomicMigration atomicMigration = new AtomicMigration(connectionFrom,
				table, connectionTo, table);
		atomicMigration.migrate();
		int destinationRowCount = getRowNumber(handlerTo, table);
		assertEquals(sourceRowCount, destinationRowCount);
	}

	@Test
	public void testTableWithInserts() throws SQLException, MigrationException,
			LocalQueryExecutionException, InterruptedException,
			ExecutionException {
		String table = "lineitem";
		prepareTable(table);
		int sourceRowCount = getRowNumber(handlerFrom, table);
		AtomicMigration atomicMigration = new AtomicMigration(connectionFrom,
				table, connectionTo, table);
		ExecutorService executor = Executors.newFixedThreadPool(2);
		Future<?> migration = executor.submit(() -> {
			try {
				TimeUnit.SECONDS.sleep(2);
				atomicMigration.migrate();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		Callable<Integer> task = () -> {
			/* how many inserts were done */
			int counter = 0;
			try {
				for (counter = 0; counter < 10000; ++counter) {
					try {
						handlerFrom.execute("insert into " + table + " values ("
								+ VALUES + ")");
					} catch (LocalQueryExecutionException e) {
						e.printStackTrace();
						return counter;
					}
					TimeUnit.MILLISECONDS.sleep(1);
				}
			} catch (InterruptedException ex) {
				ex.printStackTrace();
				return counter;
			}
			return counter;
		};
		// inserts.cancel(true);
		Future<Integer> inserts = executor.submit(task);
		int insertNumber = inserts.get();
		migration.get();
		logger.debug("Number of inserted rows: " + insertNumber);
		int destinationRowCount = getRowNumber(handlerTo, table);
		assertEquals(sourceRowCount + insertNumber, destinationRowCount);
	}

}

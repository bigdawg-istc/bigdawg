/**
 * 
 */
package istc.bigdawg.migration;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;

/**
 * Test migration of data between instances of PostgreSQL via network.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToPostgresNetworkTest
		extends FromPostgresToPostgresTest {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToPostgresNetworkTest.class);

	@Test
	public void testFromPostgresToPostgresNetworkFromLocalToRemote()
			throws Exception {
		logger.debug("Migration from local machine to madison.");
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

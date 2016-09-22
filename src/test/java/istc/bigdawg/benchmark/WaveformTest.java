/**
 *
 */
package istc.bigdawg.benchmark;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.CopyFromPostgresExecutor;
import istc.bigdawg.migration.FromPostgresToPostgres;
import istc.bigdawg.migration.FromPostgresToSciDBImplementation;
import istc.bigdawg.migration.FromSciDBToPostgresImplementation;
import istc.bigdawg.migration.LoadToSciDBExecutor;
import istc.bigdawg.migration.SciDBArrays;
import istc.bigdawg.migration.TransformBinExecutor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfoTest;

/**
 * Benchmarks for migrator.
 *
 * @author Adam Dziedzic
 */
public class WaveformTest {

	private static Logger log = Logger.getLogger(WaveformTest.class);

	private String array = "test_waveform";
	private String table = "test_waveform";
	private PostgreSQLConnectionInfo conPostgres = new PostgreSQLConnectionInfoTest();
	private SciDBConnectionInfo conSciDB = new SciDBConnectionInfoTest();

	@Before
	public void initTest() throws IOException {
		LoggerSetup.setLogging();
		log.debug("init Waveform benchmark");
	}

	@Test
	public void testFromSciDBToPostgresBin() throws MigrationException, SQLException {
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conSciDB, array, conPostgres,
				table);
		migrator.migrateBin();
	}

	@Test
	public void testFromSciDBToPostgresCsv() throws MigrationException, SQLException {
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conSciDB, array, conPostgres,
				table);
		migrator.migrateSingleThreadCSV();
	}

	@Test
	public void testFromPostgresToSciDBBin() throws MigrationException, SQLException {
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(conPostgres, table, conSciDB,
				array);
		migrator.migrateBin();
	}

	@Test
	public void testFromPostgresToSciDBCsv() throws MigrationException, SQLException {
		FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(conPostgres, table, conSciDB,
				array);
		migrator.migrateSingleThreadCSV();
	}

	@Test
	public void testFromPostgresToPostgres() throws Exception {
		PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo("postgres1", "5432", "test", "pguser", "test");
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo("postgres2", "5432", "test", "pguser", "test");
		new FromPostgresToPostgres().migrate(conFrom, table, conTo, table);
	}

	@Test
	public void loadToSciDB() throws InterruptedException, ExecutionException {
		long startTimeMigration = System.currentTimeMillis();
		ExecutorService executor = Executors.newFixedThreadPool(1);
		LoadToSciDBExecutor loadExecutor = new LoadToSciDBExecutor(conSciDB, new SciDBArrays(array, null),
				"/tmp/scidb.bin","int64,int64,double");
		FutureTask<String> loadTask = new FutureTask<String>(loadExecutor);
		executor.submit(loadTask);
		String loadMessage = loadTask.get();
		log.debug(loadMessage);
		executor.shutdown();
		long endTimeMigration = System.currentTimeMillis();
		long durationMsec = endTimeMigration - startTimeMigration;
		log.debug("loading to SciDB (msec): " + durationMsec);
	}

	@Test
	public void exportPostgres() throws SQLException, InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		String copyFromCommand = PostgreSQLHandler.getExportBinCommand(table);
		CopyFromPostgresExecutor exportExecutor = new CopyFromPostgresExecutor(conPostgres, copyFromCommand,
				"/tmp/postgres.bin");
		FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
		executor.submit(exportTask);
		long countExtractedElements = exportTask.get();
		log.debug("number of extracted rows: " + countExtractedElements);
		executor.shutdown();
	}

	@Test
	public void transformPostgresScidb() throws InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		TransformBinExecutor transformExecutor = new TransformBinExecutor("/tmp/postgres.bin", "/tmp/scidb.bin",
				"int64,int64,double", TransformBinExecutor.TYPE.FromPostgresToSciDB);
		FutureTask<Long> transformTask = new FutureTask<Long>(transformExecutor);
		executor.submit(transformTask);
		transformTask.get();
		executor.shutdown();
	}

}
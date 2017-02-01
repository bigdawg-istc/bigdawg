/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

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
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfoTest;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Utils;

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
	public void initTest() throws IOException, SQLException {
		LoggerSetup.setLogging();
		log.debug("init Waveform benchmark");
		TestMigrationUtils.loadDataToPostgresWaveform(conPostgres, table);
	}

	@Test
	public void testFromSciDBToPostgresBin()
			throws MigrationException, SQLException {
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conSciDB, array, conPostgres, table));
		migrator.migrateBin();
	}

	@Test
	public void testFromSciDBToPostgresCsv()
			throws MigrationException, SQLException {
		FromSciDBToPostgres migrator = new FromSciDBToPostgres(
				new MigrationInfo(conSciDB, array, conPostgres, table));
		migrator.migrateSingleThreadCSV();
	}

	@Test
	public void testFromPostgresToSciDBBin()
			throws MigrationException, SQLException {
		FromPostgresToSciDB migrator = new FromPostgresToSciDB(
				new MigrationInfo(conPostgres, table, conSciDB, array));
		migrator.migrateBin();
	}

	@Test
	public void testFromPostgresToSciDBCsv()
			throws MigrationException, SQLException {
		SciDBHandler.dropArrayIfExists(conSciDB, array);
		FromPostgresToSciDB migrator = new FromPostgresToSciDB(
				new MigrationInfo(conPostgres, table, conSciDB, array));
		migrator.migrateSingleThreadCSV();
		long numberOfCellsSciDBFlat = Utils.getNumberOfCellsSciDB(conSciDB,
				array);
		log.debug("Final number of cells in SciDB: " + numberOfCellsSciDBFlat);
		assertEquals(TestMigrationUtils.WAVEFORM_ROWS_NUMBER,
				numberOfCellsSciDBFlat);
		/* remove the destination array */
		SciDBHandler.dropArrayIfExists(conSciDB, array);
	}

	@Test
	public void testFromPostgresToPostgres() throws Exception {
		PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "pguser", "test");
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5430", "test", "pguser", "test");
		PostgreSQLHandler handlerTo = new PostgreSQLHandler(conTo);
		handlerTo.dropDataSetIfExists(table);
		new FromPostgresToPostgres()
				.migrate(new MigrationInfo(conFrom, table, conTo, table));

		long numberOfRowsPostgresTo = Utils.getPostgreSQLCountTuples(conTo,
				table);
		/* check if all the rows were loaded to the destination table */
		assertEquals(TestMigrationUtils.WAVEFORM_ROWS_NUMBER,
				numberOfRowsPostgresTo);
		/* remove the destination table */
		handlerTo.dropDataSetIfExists(table);
		/* remove the source table */
		PostgreSQLHandler handlerFrom = new PostgreSQLHandler(conFrom);
		handlerFrom.dropSchemaIfExists(table);
	}

	@Test
	public void loadToSciDB() throws InterruptedException, ExecutionException {
		long startTimeMigration = System.currentTimeMillis();
		ExecutorService executor = Executors.newFixedThreadPool(1);
		MigrationInfo migrationInfo = MigrationInfo.forConnectionTo(conSciDB);
		LoadSciDB loadExecutor = new LoadSciDB(migrationInfo,
				new PostgreSQLHandler(conPostgres), "/tmp/scidb.bin",
				"int64,int64,double");
		FutureTask<Object> loadTask = new FutureTask<Object>(loadExecutor);
		executor.submit(loadTask);
		String loadMessage = (String) loadTask.get();
		log.debug(loadMessage);
		executor.shutdown();
		long endTimeMigration = System.currentTimeMillis();
		long durationMsec = endTimeMigration - startTimeMigration;
		log.debug("loading to SciDB (msec): " + durationMsec);
	}

	@Test
	public void exportPostgres()
			throws SQLException, InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		String copyFromCommand = PostgreSQLHandler.getExportBinCommand(table);
		ExportPostgres exportExecutor = new ExportPostgres(conPostgres,
				copyFromCommand, "/tmp/postgres.bin",
				new PostgreSQLHandler(conPostgres));
		FutureTask<Object> exportTask = new FutureTask<Object>(exportExecutor);
		executor.submit(exportTask);
		long countExtractedElements = (long) exportTask.get();
		log.debug("number of extracted rows: " + countExtractedElements);
		executor.shutdown();
	}

	@Test
	public void transformPostgresScidb()
			throws InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		TransformBinExecutor transformExecutor = new TransformBinExecutor(
				"/tmp/postgres.bin", "/tmp/scidb.bin", "int64,int64,double",
				TransformBinExecutor.TYPE.FromPostgresToSciDB);
		FutureTask<Long> transformTask = new FutureTask<Long>(
				transformExecutor);
		executor.submit(transformTask);
		transformTask.get();
		executor.shutdown();
	}

}
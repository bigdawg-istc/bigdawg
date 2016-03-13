/**
 * 
 */
package istc.bigdawg.benchmark;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.CopyFromPostgresExecutor;
import istc.bigdawg.migration.FromPostgresToPostgres;
import istc.bigdawg.migration.FromPostgresToSciDBImplementation;
import istc.bigdawg.migration.FromSciDBToPostgresImplementation;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfoTest;

/**
 * Benchmarks for migrator.
 * 
 * @author Adam Dziedzic
 */
public class WaveformTest {

	private static Logger log = Logger.getLogger(CopyFromPostgresExecutor.class);

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
		PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo("localhost", "5431", "test", "pguser", "test");
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo("localhost", "5430", "test", "pguser", "test");
		new FromPostgresToPostgres().migrate(conFrom, table, conTo, table);
	}

}
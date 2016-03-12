/**
 * 
 */
package istc.bigdawg.benchmark;

import java.sql.SQLException;

import org.junit.Test;

import istc.bigdawg.exceptions.MigrationException;
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

	private String array = "waveform";
	private String table = "waveform";
	private PostgreSQLConnectionInfo conPostgres = new PostgreSQLConnectionInfoTest();
	private SciDBConnectionInfo conSciDB = new SciDBConnectionInfoTest();

	@Test
	public void testFromSciDBBin() throws MigrationException, SQLException {
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conSciDB, array,
				conPostgres, table);
		migrator.migrateBin();
	}

	@Test
	public void testFromSciDBCsv() throws MigrationException, SQLException {
		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conSciDB, array,
				conPostgres, table);
		migrator.migrateSingleThreadCSV();
	}

}

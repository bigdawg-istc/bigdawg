/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Test;

import istc.bigdawg.LoggerSetupForTests;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.scidb.SciDBConnectionInfo;

/**
 * Test the transformation from CSV format to SciDB format (this is a text
 * format that SciDB accepts.
 * 
 * @author Adam Dziedzic
 */
public class TransformFromCsvToSciDBExecutorTest {

	static Logger log = Logger.getLogger(TransformFromCsvToSciDBExecutorTest.class);
	
	@Test
	public void testTransformation() throws MigrationException, IOException {
		LoggerSetupForTests.setTestLogging();
		SciDBConnectionInfo connectionInfo = new SciDBConnectionInfo();
		String scidbBinPath = connectionInfo.getBinPath();
		String typesPattern = "NSS";
		String csvFilePath = "src/test/resources/region.csv";
		String scidbFilePath = "src/test/resources/region_test.scidb";
		String delimiter = "|";
		TransformFromCsvToSciDBExecutor executor = new TransformFromCsvToSciDBExecutor(typesPattern, csvFilePath,
				delimiter, scidbFilePath, scidbBinPath);
		int result = executor.call();
		assertEquals(0, result);
	}

}

/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.scidb.SciDBConnectionInfo;

/**
 * Test the transformation from CSV format to SciDB format (this is a text
 * format that SciDB accepts.
 * 
 * @author Adam Dziedzic
 */
public class TransformFromCsvToSciDBExecutorTest {

	static Logger log = Logger
			.getLogger(TransformFromCsvToSciDBExecutorTest.class);
	
	@Before
	public void setUp() {
		LoggerSetup.setLogging();
	}

	@Test
	public void testTransformation() throws MigrationException, IOException {
		SciDBConnectionInfo connectionInfo = new SciDBConnectionInfo();
		String scidbBinPath = connectionInfo.getBinPath();
		String typesPattern = "NSS";
		String csvFilePath = "src/test/resources/region.csv";
		String scidbFilePath = "src/test/resources/region_test.scidb";
		String scidbFileExemplarPath = "src/test/resources/region.scidb";
		String delimiter = "|";
		TransformFromCsvToSciDBExecutor executor = new TransformFromCsvToSciDBExecutor(
				typesPattern, csvFilePath, delimiter, scidbFilePath,
				scidbBinPath);
		long result = executor.call();
		assertEquals(0L, result);
		File scidbFile = new File(scidbFilePath);
		File scidbFileExemplar = new File(scidbFileExemplarPath);
		assertTrue(FileUtils.contentEquals(scidbFile, scidbFileExemplar));
	}

}

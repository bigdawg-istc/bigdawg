package istc.bigdawg.plan;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import istc.bigdawg.postgresql.PostgreSQLHandler;
import junit.framework.TestCase;

public class CreateTableTest extends TestCase {

	
	private static Map<String, String> expectedOutputs;
	private static PostgreSQLHandler psqlh;
	
	protected void setUp() throws Exception {
		expectedOutputs = new HashMap<>();
		psqlh			= new PostgreSQLHandler();
		
		setupCreateTableEngines();
	}
	
	
	private void setupCreateTableEngines() {
		expectedOutputs.put("catalog.engines", "CREATE TABLE catalog.engines (eid integer, name character varying(15), host character varying(40), port integer, connection_properties character varying(100));");
	}
	
	@Test
	public void testCreateTableEngines() throws Exception {
		testCaseCreateTable("catalog.engines");
	}
	
	
	private void testCaseCreateTable(String testName) throws Exception {
		
		String expectedOutput = expectedOutputs.get(testName);
		
		String serverOutput = psqlh.getCreateTable(testName);
		
		assertEquals(expectedOutput, serverOutput);
	}

}

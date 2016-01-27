package istc.bigdawg.plan;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.packages.CrossIslandQueryNode;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.parsers.UserQueryParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;

public class QEPConstruction {

	private static Map<String, String> inputs;
	private static Map<String, Object> expectedOutputs;
	
	@Before
	public void setUp() throws Exception {
		inputs = new HashMap<>();
		expectedOutputs = new HashMap<>();
		CatalogInstance.INSTANCE.getCatalog();
		
		setupCrossIslandPlanConstructionTier1();
		setupCrossIslandPlanConstructionTier2();
		setupCrossIslandPlanConstructionTier3();
		setupCrossIslandPlanConstructionTier3NoOn();
	}

	
	private void setupCrossIslandPlanConstructionTier1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT BIGDAWGPRUNED_1.id, BIGDAWGPRUNED_1.lastname, BIGDAWGPRUNED_1.firstname, BIGDAWGPRUNED_2.id, BIGDAWGPRUNED_2.disease_name FROM BIGDAWGPRUNED_2 JOIN BIGDAWGPRUNED_1 ON BIGDAWGPRUNED_2.id = BIGDAWGPRUNED_1.id");
		
		expectedOutputs.put("cross-1", ba1);
		inputs.put("cross-1", "bdrel(select * from mimic2v26.d_patients join ailment on mimic2v26.d_patients.id = ailment.id)");
	}
	
	
	private void setupCrossIslandPlanConstructionTier2() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT demographics.patient_id, vitals.height_timestamp, medications.medication FROM medications JOIN vitals ON vitals.patient_id = medications.patient_id JOIN demographics ON medications.patient_id = demographics.patient_id");
		
		expectedOutputs.put("cross-2", ba1);
		inputs.put("cross-2", "bdrel(SELECT demographics.patient_id, height_timestamp, medication FROM demographics JOIN medications ON medications.patient_id = demographics.patient_id JOIN vitals ON vitals.patient_id = demographics.patient_id)");
	}
	
	
	private void setupCrossIslandPlanConstructionTier3() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT BIGDAWGPRUNED_4.id, BIGDAWGPRUNED_4.lastname, BIGDAWGPRUNED_6.disease_name, BIGDAWGPRUNED_3.content FROM BIGDAWGPRUNED_4 JOIN BIGDAWGPRUNED_3 ON BIGDAWGPRUNED_4.id = BIGDAWGPRUNED_3.id JOIN BIGDAWGPRUNED_6 ON BIGDAWGPRUNED_4.id = BIGDAWGPRUNED_6.id JOIN BIGDAWGPRUNED_5 ON BIGDAWGPRUNED_6.disease_name = BIGDAWGPRUNED_5.disease_name");
		
		expectedOutputs.put("cross-3", ba1);
		inputs.put("cross-3", "bdrel(SELECT mimic2v26.d_patients.id, lastname, ailment.disease_name, inputs.content FROM mimic2v26.d_patients JOIN ailment ON mimic2v26.d_patients.id = ailment.id JOIN treatments ON ailment.disease_name = treatments.disease_name JOIN inputs ON mimic2v26.d_patients.id = inputs.id)");
	}
	
	private void setupCrossIslandPlanConstructionTier3NoOn() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT BIGDAWGPRUNED_9.id, BIGDAWGPRUNED_9.lastname, BIGDAWGPRUNED_8.disease_name, BIGDAWGPRUNED_8.id, BIGDAWGPRUNED_10.content FROM BIGDAWGPRUNED_7 JOIN BIGDAWGPRUNED_8 ON BIGDAWGPRUNED_8.disease_name = BIGDAWGPRUNED_7.disease_name, BIGDAWGPRUNED_9, BIGDAWGPRUNED_10");
		
		expectedOutputs.put("cross-3-no-on", ba1);
		inputs.put("cross-3-no-on", "bdrel(SELECT mimic2v26.d_patients.id, lastname, ailment.disease_name, ailment.id, inputs.content FROM mimic2v26.d_patients, ailment JOIN treatments ON ailment.disease_name = treatments.disease_name, inputs)");
	}
	
	
	@Test
	public void testCrossIslandPlanConstructionTier1() throws Exception {
		testCaseCrossIslandPlanConstruction("cross-1", false);
	}
	
	@Test
	public void testCrossIslandPlanConstructionTier2() throws Exception {
		testCaseCrossIslandPlanConstruction("cross-2", false);
	}
	
	
	@Test
	public void testCrossIslandPlanConstructionTier3() throws Exception {
		testCaseCrossIslandPlanConstruction("cross-3", false);
	}
	
	@Test
	public void testCrossIslandPlanConstructionTier3NoOn() throws Exception {
		testCaseCrossIslandPlanConstruction("cross-3-no-on", false);
	}
	

	private void testCaseCrossIslandPlanConstruction(String testName, boolean unsupportedToken) throws Exception {
		String userinput = inputs.get(testName);
		Map<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput, "BIGDAWGTAG_");
		
		CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);
		
		
		for (String k : ciqp.getMemberKeySet()) {
			CrossIslandQueryNode n = ciqp.getMember(k);
			
			String remainderText = n.getRemainder(0).generatePlaintext((Select) CCJSqlParserUtil.parse(n.getQuery()));
			System.out.println("----> Gen remainder: "+remainderText);
			assertEquals(((HashMap<String, String>)expectedOutputs.get(testName)).get("OUTPUT"), remainderText);
			
			QueryExecutionPlan qep = n.getQEP(0);
			// do something?
		}
		
		
	}
	
}

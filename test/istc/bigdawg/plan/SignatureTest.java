package istc.bigdawg.plan;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.packages.CrossIslandQueryNode;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.utils.IslandsAndCast.Scope;
import junit.framework.TestCase;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;

public class SignatureTest extends TestCase {

	private static Map<String, String> inputs;
	private static Map<String, Object> expectedOutputs;
	
	@Before
	protected void setUp() throws Exception {
		inputs = new HashMap<>();
		expectedOutputs = new HashMap<>();
		CatalogInstance.INSTANCE.getCatalog();
		
		setupBreakApartTestOne();
		setupCrossIslandPlanConstructionTier1();
		setupCrossIslandPlanConstructionTier2();
		setupCrossIslandPlanConstructionTier2WithOrderBy();
		setupCrossIslandPlanConstructionArrayTier1();
	}
	
	
	private void setupBreakApartTestOne() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "bdarray(project(BIGDAWGTAG_0, lastname, id))");
		ba1.put("BIGDAWGTAG_0", "bdrel(select * from mimic2v26.d_patients join ailment on mimic2v26.d_patients.id = ailment.id)");
		
		expectedOutputs.put("break-apart-1", ba1);
		inputs.put("break-apart-1", "bdarray(project(bdrel(select * from mimic2v26.d_patients join ailment on mimic2v26.d_patients.id = ailment.id), lastname, id));");
	}
	
	private void setupCrossIslandPlanConstructionTier1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT * FROM BIGDAWGPRUNED_2 JOIN BIGDAWGPRUNED_1 ON BIGDAWGPRUNED_2.id = BIGDAWGPRUNED_1.id");
		
		expectedOutputs.put("cross-1", ba1);
		inputs.put("cross-1", "bdrel(select * from mimic2v26.d_patients join ailment on mimic2v26.d_patients.id = ailment.id)");
	}
	
	
	private void setupCrossIslandPlanConstructionTier2() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT demographics.patient_id, vitals.height_timestamp, medications.medication FROM vitals JOIN medications ON vitals.patient_id = medications.patient_id JOIN demographics ON medications.patient_id = demographics.patient_id");
		
		expectedOutputs.put("cross-2", ba1);
		inputs.put("cross-2", "bdrel(SELECT demographics.patient_id, height_timestamp, medication FROM demographics JOIN medications ON demographics.patient_id = medications.patient_id JOIN vitals ON demographics.patient_id = vitals.patient_id)");
	}
	
	// This will fail because there is a OrderBy 
	private void setupCrossIslandPlanConstructionTier2WithOrderBy() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT demographics.patient_id, vitals.height_timestamp, medications.medication FROM medications JOIN vitals ON vitals.patient_id = medications.patient_id JOIN demographics ON medications.patient_id = demographics.patient_id ORDER BY demographics.patient_id");
		
		expectedOutputs.put("cross-2-ob", ba1);
		inputs.put("cross-2-ob", "bdrel(SELECT demographics.patient_id, height_timestamp, medication FROM demographics JOIN medications ON demographics.patient_id = medications.patient_id JOIN vitals ON demographics.patient_id = vitals.patient_id ORDER BY demographics.patient_id)");
	}
	
	private void setupCrossIslandPlanConstructionArrayTier1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT demographics.patient_id, vitals.height_timestamp, medications.medication FROM vitals JOIN medications ON vitals.patient_id = medications.patient_id JOIN demographics ON medications.patient_id = demographics.patient_id");
		
		expectedOutputs.put("array-1", ba1);
		inputs.put("array-1", "bdarray(project(mimic2v26.d_patients, mimic2v26.d_patients.id, mimic2v26.d_patients.lastname))");
	}
	
	@Test
	public void testBreakApart() throws Exception {
		testCaseForBreakApart("break-apart-1");
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
	public void testCrossIslandPlanConstructionTier2WithOrderBy() throws Exception {
		testCaseCrossIslandPlanConstruction("cross-2-ob", true);
	}
	
	
	@Test
	public void testCrossIslandPlanConstructionArrayTier1() throws Exception {
		testCaseCrossIslandPlanConstruction("array-1", false);
	}
	
	private void testCaseForBreakApart(String testName) throws Exception {
		
		String userinput = inputs.get(testName);
		Map<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput);
		
		assertEquals(expectedOutputs.get(testName), crossIslandQuery);
	}

	private void testCaseCrossIslandPlanConstruction(String testName, boolean unsupportedToken) throws Exception {
		String userinput = inputs.get(testName);
		LinkedHashMap<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput);
		
		try { 
			CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);
			
			if (unsupportedToken) fail("Failed to throw exception about unsupported token");
			
			for (String k : ciqp.getMemberKeySet()) {
				CrossIslandQueryNode n = ciqp.getMember(k);
				String remainderText = ""; 
				if (n.getScope().equals(Scope.RELATIONAL))
					remainderText = n.getRemainder(0).generateSQLString((Select) CCJSqlParserUtil.parse(n.getQuery()));
				else if (n.getScope().equals(Scope.ARRAY))
					remainderText = n.getRemainder(0).generateAFLString(0);
				System.out.println("----> Gen remainder: "+remainderText+"\n");
				assertEquals(((HashMap<String, String>)expectedOutputs.get(testName)).get("OUTPUT"), remainderText);
			}
			
		} catch (Exception e) {
			if (unsupportedToken) 
				assertEquals("unsupported Operator in CrossIslandQueryNode", e.getMessage());
			else {
				e.printStackTrace();
				fail("Expecting an exception about unsupported operation");
			}
		}
		
		
	}
	
}

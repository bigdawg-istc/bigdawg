package istc.bigdawg.plan;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.packages.CrossIslandQueryNode;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.packages.QueryContainerForCommonDatabase;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.utils.IslandsAndCast.Scope;
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
		setupCrossIslandPlanConstructionTier4_1();
		setupCrossIslandPlanConstructionTier4_2();
		setupCrossIslandPlanConstructionTier5_1();
		setupCrossIslandPlanConstructionTier6();
	}

	
	private void setupCrossIslandPlanConstructionTier1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT BIGDAWGPRUNED_1.id, BIGDAWGPRUNED_1.lastname, BIGDAWGPRUNED_2.disease_name FROM BIGDAWGPRUNED_2 JOIN BIGDAWGPRUNED_1 ON BIGDAWGPRUNED_2.id = BIGDAWGPRUNED_1.id");
		
		expectedOutputs.put("cross-1", ba1);
		inputs.put("cross-1", "bdrel(select mimic2v26.d_patients.id, mimic2v26.d_patients.lastname, ailment.disease_name from mimic2v26.d_patients join ailment on mimic2v26.d_patients.id = ailment.id)");
	}
	
	
	private void setupCrossIslandPlanConstructionTier2() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT demographics.patient_id, vitals.height_timestamp, medications.medication FROM vitals JOIN medications ON vitals.patient_id = medications.patient_id JOIN demographics ON medications.patient_id = demographics.patient_id");
		
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
		ba1.put("OUTPUT", "SELECT BIGDAWGPRUNED_9.id, BIGDAWGPRUNED_9.lastname, BIGDAWGPRUNED_8.disease_name, BIGDAWGPRUNED_8.id, BIGDAWGPRUNED_10.content FROM BIGDAWGPRUNED_8 JOIN BIGDAWGPRUNED_7 ON BIGDAWGPRUNED_8.disease_name = BIGDAWGPRUNED_7.disease_name, BIGDAWGPRUNED_10, BIGDAWGPRUNED_9");
		
		expectedOutputs.put("cross-3-no-on", ba1);
		inputs.put("cross-3-no-on", "bdrel(SELECT mimic2v26.d_patients.id, lastname, ailment.disease_name, ailment.id, inputs.content FROM mimic2v26.d_patients, ailment JOIN treatments ON ailment.disease_name = treatments.disease_name, inputs)");
	}
	
	private void setupCrossIslandPlanConstructionTier4_1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT demographics.patient_id, vitals.height_timestamp, medications.medication FROM vitals JOIN medications ON vitals.patient_id = medications.patient_id JOIN demographics ON medications.patient_id = demographics.patient_id ORDER BY demographics.patient_id, vitals.height_timestamp");
		
		expectedOutputs.put("order-by-1", ba1);
		inputs.put("order-by-1", "bdrel(SELECT demographics.patient_id, height_timestamp, medication FROM demographics JOIN medications ON medications.patient_id = demographics.patient_id JOIN vitals ON vitals.patient_id = demographics.patient_id order by demographics.patient_id, height_timestamp)");
	}
	
	private void setupCrossIslandPlanConstructionTier4_2() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT BIGDAWGPRUNED_13.id, BIGDAWGPRUNED_13.lastname, BIGDAWGPRUNED_12.disease_name, BIGDAWGPRUNED_12.id, BIGDAWGPRUNED_14.content FROM BIGDAWGPRUNED_12 JOIN BIGDAWGPRUNED_11 ON BIGDAWGPRUNED_12.disease_name = BIGDAWGPRUNED_11.disease_name, BIGDAWGPRUNED_14, BIGDAWGPRUNED_13 ORDER BY BIGDAWGPRUNED_13.id, BIGDAWGPRUNED_13.lastname");
		
		expectedOutputs.put("order-by-2", ba1);
		inputs.put("order-by-2", "bdrel(SELECT mimic2v26.d_patients.id, lastname, ailment.disease_name, ailment.id, inputs.content FROM mimic2v26.d_patients, ailment JOIN treatments ON ailment.disease_name = treatments.disease_name, inputs order by mimic2v26.d_patients.id, lastname)");
	}
	
	private void setupCrossIslandPlanConstructionTier5_1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "cross_join(BIGDAWGPRUNED_2, BIGDAWGPRUNED_1)");
		
		expectedOutputs.put("array1", ba1);
		inputs.put("array1", "bdarray(cross_join(go_matrix, genes))");
	}
	
	private void setupCrossIslandPlanConstructionTier6() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "cross_join(BIGDAWGPRUNED_2, BIGDAWGPRUNED_1)");
		
		expectedOutputs.put("relarray1", ba1);
		inputs.put("relarray1", "bdrel(select * from patients join geo on patients.id = geo.patientid join bdarray(cross_join(go_matrix, genes)) as g on g.id = geo.geneid)");
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
	
	@Test
	public void testCrossIslandPlanConstructionTier4_1() throws Exception {
		testCaseCrossIslandPlanConstruction("order-by-1", false);
	}
	
	@Test
	public void testCrossIslandPlanConstructionTier4_2() throws Exception {
		testCaseCrossIslandPlanConstruction("order-by-2", false);
	}
	
	@Test
	public void testCrossIslandPlanConstructionTier5_1() throws Exception {
		testCaseCrossIslandPlanConstruction("array1", false);
	}
	
	@Test
	public void testCrossIslandPlanConstructionTier6() throws Exception {
		testCaseCrossIslandPlanConstruction("relarray1", false);
	}
	

	@SuppressWarnings("unchecked")
	private void testCaseCrossIslandPlanConstruction(String testName, boolean unsupportedToken) throws Exception {
		String userinput = inputs.get(testName);
		LinkedHashMap<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput);
		
		CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);
		
		System.out.println("\n\n\nMember KeySet Size: "+ciqp.getMemberKeySet().size()+"\n\n");
		
		for (String k : ciqp.getMemberKeySet()) {
			
			CrossIslandQueryNode n = ciqp.getMember(k);
			String remainderText;
			Map<String, QueryContainerForCommonDatabase> container;
			
			
			
			
			System.out.println("Member: "+k+"; Island: "+n.getScope().toString());
			
			
			
			
			
			
			
			if (n.getScope().equals(Scope.RELATIONAL))
				remainderText = n.getRemainder(0).generateSQLString((Select) CCJSqlParserUtil.parse(n.getQuery()));
			else if (n.getScope().equals(Scope.ARRAY))
				remainderText = n.getRemainder(0).generateAFLString(0);
			else 
				throw new Exception("Unimplemented island: "+n.getScope().toString());
			
			
			
			
			// remainder
			if (k.toLowerCase().startsWith("bigdawgtag_")){
				System.out.println("Root schema in SQL: "+n.getRemainder(0).generateSQLCreateTableStatementLocally(k));
				System.out.println("Root schema in AFL: "+n.getRemainder(0).generateAFLCreateArrayStatementLocally(k));
			}
			
				
			// container
			container = n.getQueryContainer();
			System.out.println("----> Gen remainder: \n- "+remainderText);
			System.out.println("----> Gen container: ");
			
			
			
			for (String s: container.keySet()) {
				if (n.getScope().equals(Scope.RELATIONAL))
					System.out.println("- "+container.get(s).generateSQLSelectIntoString());
				else if (n.getScope().equals(Scope.ARRAY))
					System.out.println("- "+container.get(s).generateAFLStoreString());
				else 
					throw new Exception("Unimplemented island: "+n.getScope().toString());
			}
			System.out.println("\n");
			
			
			// do something with QEP?
		}
		
		
	}
	
}

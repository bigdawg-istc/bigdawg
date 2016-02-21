package istc.bigdawg.plan;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.plan.extract.AFLPlanParser;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.scidb.SciDBHandler;
import junit.framework.TestCase;

public class AFLParserTest extends TestCase {
	private static Map<String, String> expectedOutputs;
	
	protected void setUp() throws Exception {
		expectedOutputs = new HashMap<>();
		
		
		
		setupParse1();
		
//		setupBasicProjection();
//		setupCrossJoins();
	}
	
	private void setupParse1() {
//		expectedOutputs.put("parse1", "cross_join(project(filter(poe_med, i <= 5), poe_id, drug_type, dose_val_disp) AS a, project(filter(redimension(poe_med, <drug_type:string,drug_name:string,drug_name_generic:string,prod_strength:string,form_rx:string,dose_val_rx:string,dose_unit_rx:string,form_val_disp:string,form_unit_disp:string,dose_val_disp:double,dose_unit_disp:string,dose_range_override:string>[poe_id=0:10000000,1,1]), poe_id = 3750047), dose_val_rx) AS b)");
//		expectedOutputs.put("parse1", "sort(filter(poe_med, i <= 5), poe_id, drug_type, dose_val_disp)");
//		expectedOutputs.put("parse1", "cross_join(project(filter(poe_med, i <= 5), poe_id, drug_type, dose_val_disp) AS a, project(filter(poe_med, poe_id = 3750047), dose_val_rx) AS b, a.i, b.i)");
		
		expectedOutputs.put("parse1", "filter(patients, id < 10)");
		

	}
	
	@Test
	public void testParse1() throws Exception {
		testParse("parse1");
	}
	
//	private void setupBasicProjection() {
//		expectedOutputs.put("projection", "project(demography, demography.name, demography.id)");
//	}
//	
//	private void setupCrossJoins() {
//		expectedOutputs.put("cross_joins", "cross_join(cross_join(a, b), cross_join(c, d, c.id, d.id), a.id, c.id)");
//	}
//	
//	@Test
//	public void testBasicProjection() throws Exception {
//		testCase("projection");
//	}
//	
//	@Test
//	public void testCrossJoins() throws Exception {
//		testCase("cross_joins");
//	}
//	
//	
//	private void testCase(String testName) throws Exception {
//		
//		String expectedOutput = expectedOutputs.get(testName);
//		
//		Operator root = AFLParser.parsePlanTail(expectedOutput);
//		
//		System.out.println();
//		
//	}
	public void testParse(String testname) throws Exception {
		
		AFLQueryPlan queryPlan = AFLPlanParser.extractDirect(new SciDBHandler(CatalogViewer.getSciDBConnectionInfo(6)), expectedOutputs.get(testname));
		Operator root = queryPlan.getRootNode();
		
		System.out.println(root.printPlan(0));
	}
}

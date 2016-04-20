package istc.bigdawg.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.packages.CrossIslandQueryNode;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.plan.generators.SQLQueryGenerator;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.utils.IslandsAndCast.Scope;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.schema.Column;

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
		setupCrossIslandPlanConstructionTier6_1();
		setupCrossIslandPlanConstructionTier6_2();
		setupCrossIslandPlanConstructionAggTier1();
		setupCrossIslandPlanConstructionAggTier2();
		setupCrossIslandPlanConstructionTPCH2();
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
	
	private void setupCrossIslandPlanConstructionTier6_1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "cross_join(BIGDAWGPRUNED_2, BIGDAWGPRUNED_1)");
		
		expectedOutputs.put("relarray1", ba1);
		inputs.put("relarray1", "bdrel(select * from patients join geo on patients.id = geo.patientid join bdarray(cross_join(go_matrix, genes)) as g on g.id = geo.geneid)");
	}
	
	private void setupCrossIslandPlanConstructionTier6_2() {
//		HashMap<String, String> ba1 = new HashMap<>();
//		ba1.put("OUTPUT", "cross_join(BIGDAWGPRUNED_2, BIGDAWGPRUNED_1)");
//		
//		expectedOutputs.put("relarray2", ba1);
		inputs.put("relarray2", "bdrel(select * from patients join geo on patients.id = geo.patientid join bdarray(cross_join(go_matrix, genes)) as g on g.id = geo.geneid where g.id < 3)");
	}
	
	private void setupCrossIslandPlanConstructionAggTier1() {
//		HashMap<String, String> ba1 = new HashMap<>();
//		ba1.put("OUTPUT", "cross_join(BIGDAWGPRUNED_2, BIGDAWGPRUNED_1)");
//		
//		expectedOutputs.put("aggsql1", ba1);
		inputs.put("aggsql1", "bdrel(select p.id, avg(g.expr_value) AS a from patients AS p join geo AS g on p.id = g.patientid where p.id < 3 group by p.id order by p.id)");
	}
	
	private void setupCrossIslandPlanConstructionAggTier2() {
//		HashMap<String, String> ba1 = new HashMap<>();
//		ba1.put("OUTPUT", "cross_join(BIGDAWGPRUNED_2, BIGDAWGPRUNED_1)");
//		
//		expectedOutputs.put("aggsql1", ba1);
		inputs.put("aggsql2", "bdrel(select p.id, avg(g.expr_value)/avg(g.expr_value)+3 AS a from patients AS p join geo AS g on p.id = g.patientid where p.id < 3 group by p.id order by p.id)");
	}
	
	private void setupCrossIslandPlanConstructionTPCH2() {
		inputs.put("tpch2", "bdrel(select supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone from partsupp, part, region, supplier, nation where ((part.p_type LIKE '%BRASS') AND (part.p_size = 14)) AND (part.p_partkey = partsupp.ps_partkey) AND (supplier.s_suppkey = partsupp.ps_suppkey) AND (nation.n_nationkey = supplier.s_nationkey) AND (region.r_name = 'AMERICA') AND (region.r_regionkey = nation.n_regionkey) ORDER BY supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey)");
	};
	
	
//	@Test
//	public void testCrossIslandPlanConstructionTier1() throws Exception {
//		testCaseCrossIslandPlanConstruction("cross-1", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier2() throws Exception {
//		testCaseCrossIslandPlanConstruction("cross-2", false);
//	}
//	
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier3() throws Exception {
//		testCaseCrossIslandPlanConstruction("cross-3", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier3NoOn() throws Exception {
//		testCaseCrossIslandPlanConstruction("cross-3-no-on", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier4_1() throws Exception {
//		testCaseCrossIslandPlanConstruction("order-by-1", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier4_2() throws Exception {
//		testCaseCrossIslandPlanConstruction("order-by-2", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier5_1() throws Exception {
//		testCaseCrossIslandPlanConstruction("array1", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier6_1() throws Exception {
//		testCaseCrossIslandPlanConstruction("relarray1", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionTier6_2() throws Exception {
//		testCaseCrossIslandPlanConstruction("relarray2", false);
//	}
//	
//	@Test
//	public void testCrossIslandPlanConstructionAggTier1() throws Exception {
//		testCaseCrossIslandPlanConstruction("aggsql1", false);
//	}
//
//	@Test
//	public void testCrossIslandPlanConstructionAggTier2() throws Exception {
//		testCaseCrossIslandPlanConstruction("aggsql2", false);
//	}
	
	@Test
	public void testCrossIslandPlanConstructionTPCH2() throws Exception {
		testCaseCrossIslandPlanConstruction("tpch2", false);
	}
	

	private void testCaseCrossIslandPlanConstruction(String testName, boolean unsupportedToken) throws Exception {
		String userinput = inputs.get(testName);
		LinkedHashMap<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput);
		
		CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);
		
		
		System.out.println("\n\n\nRaw query: "+userinput);
		
		System.out.println("\n\nMember KeySet Size: "+ciqp.getMemberKeySet().size()+"\n");
		
		for (String k : ciqp.getMemberKeySet()) {
			
			CrossIslandQueryNode n = ciqp.getMember(k);
			
			System.out.println("Member: "+k+"; Island: "+n.getScope().toString());
			
			
			// schemas
			if (k.toLowerCase().startsWith("bigdawgtag_")){
				System.out.println("Root schema in SQL: \n- "+n.getRemainder(0).generateSQLCreateTableStatementLocally(k));
//				System.out.println("Root schema in AFL: \n- "+n.getRemainder(0).generateAFLCreateArrayStatementLocally(k));
			}
			
			n.printSignature();
			
			for (String s : n.getQueryContainer().keySet()){
				System.out.println("\nContainer select into: "+n.getQueryContainer().get(s).generateSQLSelectIntoString());
			};
			
			
			System.out.printf("All possible remainder permutations: count: %s\n", n.getAllRemainders().size());
			int i = 1;
			for (Operator o : n.getAllRemainders()) {
				
				printAllInterestingNodes(o);
				
//				else if (n.getScope().equals(Scope.ARRAY))
//					System.out.printf("-- %d. %s\n", i, o.generateAFLString(0));
				
				i++;
			}
			
			System.out.println("\n");
			
			 
			// qeps and benchmarks
//			Monitor.addBenchmarks(n.getAllQEPs(true), n.getSignature(), true);
			
		}
		
		
	}
	
	private void printAllInterestingNodes(Operator o) throws Exception {
		
		int generation = 1;
		
		List<Operator> walker = new ArrayList<>();
		walker.add(o);
		while (!walker.isEmpty()) {
			List<Operator> nextgen = new ArrayList<>();
			for (Operator child : walker){
				
				if (child.isPruned()) continue;
				
				StringBuilder sb = new StringBuilder();
				
				SQLQueryGenerator generator = new SQLQueryGenerator();
				Join j = generator.generateStatementForPresentNonJoinSegment(child, sb, false);
				System.out.printf("-- %s. current: %s;\n\njoin: %s\n\n\n", generation, sb, generator.generateSelectIntoStatementForExecutionTree(j.getJoinToken()));
				
				if (j != null ) {
					nextgen.addAll(j.getChildren());
				}
				
			}
			walker = nextgen;
			generation++;
		}
		
	}
}

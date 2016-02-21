package istc.bigdawg.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.schema.SQLDatabaseSingleton;
import istc.bigdawg.catalog.CatalogInitiator;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.plan.ExecutionNode;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.planner.Planner;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import junit.framework.TestCase;

public class SQLGenerationTest  extends TestCase {

	private Map<String, String> expectedPlaintexts;
	private Map<String, String> inputPlaintexts;
	public static PostgreSQLHandler psqlh = null;
	
	protected void setUp() throws Exception {
		expectedPlaintexts  = new HashMap<String, String>();
		inputPlaintexts  = new HashMap<String, String>();
		
		CatalogInstance.INSTANCE.getCatalog();
		if (psqlh == null) psqlh = new PostgreSQLHandler(0, 3);
		
		
//		SQLDatabaseSingleton.getInstance().setDatabase("bigdawg_schemas", "src/main/resources/schemas/plain.sql");
		
		// ingesting the dummy schema 
//		psqlh.populateSchemasSchema(new String(Files.readAllBytes(Paths.get("src/main/resources/plain.sql"))), false);
		
//		cc = new istc.bigdawg.catalog.Catalog();
//		CatalogInitiator.connect(cc, "jdbc:postgresql://localhost:5431/bigdawg_catalog", "pguser", "test");
/*
//		CatalogInitiator.createSchemaAndRelations(cc);
		
		CatalogModifier.addIsland(cc, "RELATIONAL", "Everything written in PSQL");
//		CatalogModifier.addEngine(cc, "PostgreSQL", "jdbc:postgresql://localhost", 5431, "N/A");
		CatalogModifier.addShim(cc, 0, 0, "N/A");
//		CatalogModifier.addDatabase(cc, 0, "healthlnk_1", "pguser", "postgres");
//		CatalogModifier.addDatabase(cc, 0, "healthlnk_2", "pguser", "postgres");
			
		CatalogModifier.addObject(cc, "demographics", "patient_id,birth_year,gender,race,ethnicity,insurance,zip", 1, 1);
		CatalogModifier.addObject(cc, "demographics", "patient_id,birth_year,gender,race,ethnicity,insurance,zip", 1, 0);
//		CatalogModifier.addObject(cc, "diagnoses", "patient_id,timestamp_,visit_no,type_,encounter_id,diag_src,icd9,primary_", 1, 1);
		CatalogModifier.addObject(cc, "diagnoses", "patient_id,timestamp_,visit_no,type_,encounter_id,diag_src,icd9,primary_", 1, 0);
		CatalogModifier.addObject(cc, "vitals", "patient_id,height_timestamp,height_visit_no,height,height_units,weight_timestamp,weight_visit_no,weight,weight_units,bmi_timestamp,bmi_visit_no,bmi,bmi_units,pulse,systolic,diastolic,bp_method)", 1, 1);
		CatalogModifier.addObject(cc, "vitals", "patient_id,height_timestamp,height_visit_no,height,height_units,weight_timestamp,weight_visit_no,weight,weight_units,bmi_timestamp,bmi_visit_no,bmi,bmi_units,pulse,systolic,diastolic,bp_method)", 1, 0);
//		CatalogModifier.addObject(cc, "labs", "patient_id,timestamp_,test_name,value_,unit,value_low,value_high", 1, 1);
		CatalogModifier.addObject(cc, "labs", "patient_id,timestamp_,test_name,value_,unit,value_low,value_high", 1, 0);
		CatalogModifier.addObject(cc, "medications", "patient_id,timestamp_,medication,dosage,route", 1, 1);
//		CatalogModifier.addObject(cc, "medications", "patient_id,timestamp_,medication,dosage,route", 1, 0);
		CatalogModifier.addObject(cc, "site", "id", 1, 1);
//		CatalogModifier.addObject(cc, "site", "id", 1, 0);
//*/		
//		setupFourWayOne();
		setupMimicBasic();
		setupFourWayOne();
	}

	private void setupMimicBasic() {
		String testName = "mimic_basic";
		final String inputPlaintext    = "bdrel(select ailment.id, mimic2v26.d_patients.lastname, mimic2v26.d_patients.firstname, ailment.disease_name from ailment join mimic2v26.d_patients on ailment.id = mimic2v26.d_patients.id);";
		final String expectedPlaintext = "SELECT ailment.id, d_patients.lastname, d_patients.firstname, ailment.disease_name FROM ailment JOIN mimic2v26.d_patients ON ailment.id = mimic2v26.d_patients.id";
		inputPlaintexts.put(testName, inputPlaintext);
		expectedPlaintexts.put(testName, expectedPlaintext);
	}
	
	private void setupFourWayOne() {
		String testName = "four_way_one";
		final String inputPlaintext    = "bdrel(WITH diag AS (SELECT * FROM diagnoses WHERE icd9 LIKE '410%' order by patient_id), dnd AS (SELECT d.patient_id, race, count(type_) AS ct FROM diag AS d JOIN demographics AS dm ON d.patient_id = dm.patient_id GROUP BY race, d.patient_id), v AS (SELECT * FROM vitals WHERE pulse > 80) SELECT dnd.race, m.medication, dnd.ct, v.patient_id FROM dnd JOIN medications AS m ON dnd.patient_id = m.patient_id JOIN v ON v.patient_id = dnd.patient_id);";
		final String expectedPlaintext = "WITH diag AS (SELECT * FROM diagnoses WHERE icd9 LIKE '410%' ORDER BY patient_id), dnd AS (SELECT d.patient_id, race, count(type_) AS ct FROM diag AS d JOIN demographics AS dm ON d.patient_id = dm.patient_id GROUP BY race, d.patient_id), v AS (SELECT * FROM vitals WHERE pulse > 80) SELECT dnd.race, m.medication, dnd.ct, v.patient_id FROM dnd JOIN v ON v.patient_id = dnd.patient_id JOIN medications AS m ON m.patient_id = dnd.patient_id";
		inputPlaintexts.put(testName, inputPlaintext);
		expectedPlaintexts.put(testName, expectedPlaintext);
	};
	/*
	private void setupAspirinRate() {
		
		String testName = "aspirin_rate";
		final String expectedPlaintext = "WITH diag_counts AS (SELECT COUNT(DISTINCT patient_id) AS d_cnt FROM diagnoses WHERE icd9 LIKE '410%'), rx_counts AS (SELECT COUNT(DISTINCT m.patient_id) AS rx_cnt FROM diagnoses d JOIN medications m ON d.patient_id = m.patient_id WHERE medication = 'aspirin' AND icd9 LIKE '410%' AND d.timestamp_ <= m.timestamp_) SELECT diag_counts.d_cnt, rx_counts.rx_cnt, diag_counts.d_cnt / rx_counts.rx_cnt FROM diag_counts, rx_counts";
		expectedPlaintexts.put(testName, expectedPlaintext);
	}
	private void setupCDiff() {
		
		final String testName = "cdiff";
		final String expectedPlaintext = "WITH diags AS (SELECT patient_id, timestamp_, row_number() OVER (PARTITION BY patient_id ORDER BY timestamp_) AS r FROM diagnoses WHERE icd9 = '008.45') SELECT DISTINCT d1.patient_id FROM diags AS d1 JOIN diags d2 ON (d1.patient_id = d2.patient_id AND d1.r + 1 = d2.r) WHERE (((d2.timestamp_ - d1.timestamp_) >= INTERVAL '15 days') AND ((d2.timestamp_ - d1.timestamp_) <= INTERVAL '56 days'))";
		
		expectedPlaintexts.put(testName, expectedPlaintext);
		
	}
	private void setupComorbidity() {
		final String testName = "comorbidity";
		final String expectedPlaintext = "SELECT icd9, count(*) AS cnt FROM diagnoses WHERE ((diagnoses.icd9 <> '008.45') AND (diagnoses.patient_id IN (1, 2, 5))) GROUP BY icd9 ORDER BY cnt DESC";
		
		expectedPlaintexts.put(testName, expectedPlaintext);
		
	}
//*/
	/*
	@Test
	public void testMimicBasic() throws Exception {
			testCaseDirect("mimic_basic");
	}
	//*/
	
	@Test
	public void testFourWayOneDirect() throws Exception {
			testCaseDirect("four_way_one");
	}
	/*
	@Test
	public void testAspirinRateDirect() throws Exception {
			testCaseDirect("aspirin_rate");
	}
	@Test
	public void testCDiffDirect() throws Exception {
			testCaseDirect("cdiff");
	}

	@Test
	public void testComorbidityDirect() throws Exception {
			testCaseDirect("comorbidity");
	}
	//*/

	private void testCaseDirect(String testName) throws Exception {
		
		
		String sqlQuery = inputPlaintexts.get(testName);
		
		
		
	
		
		
		// UNROLLING
		Map<String, String> islandQueries = UserQueryParser.getUnwrappedQueriesByIslands(sqlQuery, "BIGDAWGTAG_");
		
		
		// GET SIGNATURE AND CASTS
		HashMap<String, Object> sigsCasts = (HashMap<String, Object>) UserQueryParser.getSignaturesAndCasts(islandQueries);
		
		
		// GET ALTERNATIVE EXECUTION PLANS
		ArrayList<String> objs = new ArrayList<>(Arrays.asList(((Signature) sigsCasts.get("OUTPUT")).getSig2().split("\t")));
		Map<String, ArrayList<String>> map = CatalogViewer.getDBMappingByObj(objs);
		CatalogInstance.INSTANCE.closeCatalog();
		
		
		System.out.println("===>>>> "+map.toString() + " <<<<===");
		
		
		
		// generating query tree 
		SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, ((Signature)sigsCasts.get("OUTPUT")).getQuery());
		Operator root = queryPlan.getRootNode();
		Map<String, ArrayList<String>> rootTL = root.getTableLocations(map);
		
		
		
		
		String sql = root.generatePlaintext(queryPlan.getStatement()); // the production of AST should be root
		System.out.println("statement: " + queryPlan.getStatement());
		System.out.println("generatedPlaintext: " + sql);
		assertEquals(expectedPlaintexts.get(testName), sql);

		
		
		Map<String, Operator> out =  ExecutionNodeFactory.traverseAndPickOutWiths(root);
		for (String s : out.keySet()){
			System.out.println("---->>>> " + s +"; " + out.get(s).generatePlaintext(queryPlan.getStatement()));
		}
		//*/
		
		//System.out.println("RESULT: \n"+ Planner.processQuery(sqlQuery));
		
		
		
		// demo calculating partial plans
		int level = 0;
		System.out.println("\nLEVEL " + level + " :: "+ root.toString() + " :: \n==>\n" + root.generatePlaintext(queryPlan.getStatement()));
		System.out.println("TABLE LOCATIONS: "+rootTL); // this getTableLocations(...) function modifies map
		System.out.println("---===>>> "+root.getClass().toString());
		System.out.println("---===>>> PARENT "+root.getParent());
		
//		for (Operator o : ExecutionNodeFactory.joinGrouper((Join)root)) {
//			if (o instanceof CommonSQLTableExpressionScan) {
//				CommonSQLTableExpressionScan c = (CommonSQLTableExpressionScan) o;
//				System.out.println("->->->"+(c.getTable().getName()) + " ---- " + map.get(c.getTable().getName()));
//				
//			} else {
//				System.out.println("->->->"+o.toString());
//			}
//		};
		System.out.println("\n\n\n"+map.toString()+"\n\n");
		
		
		level++;
		List<Operator> treeWalker = root.getChildren();
		while(treeWalker.size() > 0) {
			List<Operator> nextGeneration = new ArrayList<Operator>();
			for(Operator c : treeWalker) {
				
				String plaintext = c.generatePlaintext(queryPlan.getStatement());
				System.out.println("\nLEVEL "+ level +" :: "+c.blockingStatus()+" :: "+ c.toString() + " \n==>>\n" + plaintext);
				System.out.println("TABLE LOCATIONS: "+c.getTableLocations(map));
				System.out.println("---===>>> "+c.getClass().toString());
				System.out.println("---===>>> PARENT "+c.getParent());
				// blocking status acquired
				
				
				nextGeneration.addAll(c.getChildren());
				if(c instanceof CommonSQLTableExpressionScan) {
					nextGeneration.add(((CommonSQLTableExpressionScan) c).getSourceStatement());
				}
			}
			treeWalker = nextGeneration;
			level++;
		}
		
		//*/
		
		System.out.println("\n\n\n");
		
		// EXECUTE QUERY TREE OF SUB-PLAN 
		// TODO
		// TODO
		// TODO
	}
	
	
}

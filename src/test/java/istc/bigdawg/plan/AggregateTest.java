package istc.bigdawg.plan;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.SQLPlanParser;
import istc.bigdawg.islands.relational.SQLQueryGenerator;
import istc.bigdawg.islands.relational.SQLQueryPlan;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;

public class AggregateTest {

	private static Map<String, String> inputs;
	private static Map<String, Map<String, String>> expectedOutputs;
	private static PostgreSQLHandler psqlh;
	
	@Before
	public void setUp() throws Exception {
		
		inputs = new HashMap<>();
		expectedOutputs = new HashMap<>();
		CatalogInstance.INSTANCE.getCatalog();
		psqlh = new PostgreSQLHandler(3);
		
		setupAggTier1();
		setupAggTier2();
		setupAggTier3();
		setupAggTier4();
	}
	
	private void setupAggTier1() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT gender, avg(response) AS avg_r, count(gender) AS avg_gender FROM patients GROUP BY gender, disease");
		
		expectedOutputs.put("aggTier1", ba1);
		inputs.put("aggTier1", "SELECT gender, avg(response) AS avg_r, count(gender) AS avg_gender FROM patients GROUP BY gender, disease;");
	}
	
	private void setupAggTier2() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT gender, avg(response) / 3 AS avg_r, count(gender) AS avg_gender FROM patients GROUP BY gender, disease");
		
		expectedOutputs.put("aggTier2", ba1);
		inputs.put("aggTier2", "SELECT gender, avg(response)/3 AS avg_r, count(gender) AS avg_gender FROM patients GROUP BY gender, disease;");
	}
	
	private void setupAggTier3() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT gender, avg(response) / avg(gender) + 3 AS avg_r, count(gender) AS avg_gender FROM patients GROUP BY gender, disease");
		
		expectedOutputs.put("aggTier3", ba1);
		inputs.put("aggTier3", "SELECT gender, avg(response)/avg(gender)+3 AS avg_r, count(gender) AS avg_gender FROM patients GROUP BY gender, disease;");
	}
	
	private void setupAggTier4() {
		HashMap<String, String> ba1 = new HashMap<>();
		ba1.put("OUTPUT", "SELECT p1.gender, avg(p1.response) + 3 AS avg_r, count(p2.gender) AS avg_gender FROM patients AS p1 JOIN patients AS p2 ON p1.id = p2.id WHERE p1.id <= p2.id AND (p2.gender > 0) GROUP BY p1.gender, p2.disease");
		
		expectedOutputs.put("aggTier4", ba1);
		inputs.put("aggTier4", "SELECT p1.gender, avg(p1.response)+3 AS avg_r, count(p2.gender) AS avg_gender FROM patients AS p1 JOIN patients AS p2 ON p1.id = p2.id or p1.id <= p2.id where p2.gender > 0 GROUP BY p1.gender, p2.disease;");
	}

//	@Test
//	public void testAgg1() {
//		runTestCase("aggTier1");
//	}
//	
//	@Test
//	public void testAgg2() {
//		runTestCase("aggTier2");
//	}
//	
//	@Test
//	public void testAgg3() {
//		runTestCase("aggTier3");
//	}

	@Test
	public void testAgg4() {
		runTestCase("aggTier4");
	}
	
	
	public void runTestCase(String testname) {
		try {
			SQLQueryPlan qp = SQLPlanParser.extractDirectFromPostgreSQL(psqlh, inputs.get(testname));
			Operator root = qp.getRootNode();
			
			SQLQueryGenerator gen = new SQLQueryGenerator();
			gen.configure(true, false);
			gen.setSrcStatement(((Select)CCJSqlParserUtil.parse(inputs.get(testname))));
			root.accept(gen);
			
			System.out.printf("Original query   : %s\n", inputs.get(testname));
			System.out.printf("Root query output: %s\n", gen.generateStatementString());
			
			assertEquals(expectedOutputs.get(testname).get("OUTPUT"), gen.generateStatementString());
			
//			System.out.printf("AFL: "+root.generateAFLString(0));
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
	}
	
	

}

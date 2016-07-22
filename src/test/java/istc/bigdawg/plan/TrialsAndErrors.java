package istc.bigdawg.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.islands.OperatorVisitor;
import istc.bigdawg.islands.TheObjectThatResolvesAllDifferencesAmongTheIslands;
import istc.bigdawg.islands.Accumulo.AccumuloD4MParser;
import istc.bigdawg.islands.SStore.SStoreQueryParser;
import istc.bigdawg.islands.SciDB.AFLPlanParser;
import istc.bigdawg.islands.SciDB.AFLQueryGenerator;
import istc.bigdawg.islands.SciDB.AFLQueryPlan;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.SQLPlanParser;
import istc.bigdawg.islands.relational.SQLQueryGenerator;
import istc.bigdawg.islands.relational.SQLQueryPlan;
import istc.bigdawg.migration.MigrationParams;
import istc.bigdawg.migration.Migrator;
import istc.bigdawg.planner.Planner;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.signature.Signature;
import net.sf.jsqlparser.statement.select.Select;

public class TrialsAndErrors {
	
	private static boolean runExplainer = false;
	private static boolean runBuilder = false;
	private static boolean runRegex = false;
	private static boolean runWalker = false;
	private static boolean runPlanner = false;
	private static boolean runMapTrial = false;
	private static boolean runSchemaGen = false;
	private static boolean runMigration = false;
	private static boolean runSciDBExecution = false;
	private static boolean runParserTest = false;

	@Before
	public void setUp() throws Exception {
		CatalogInstance.INSTANCE.getCatalog();
		
//		setupQueryExplainer();
//		setupQueryBuilder();
//		setupRegexTester();
//		setupTreeWalker();
//		setupPlannerTester();
//		setupMapTrial();
//		setupSchemaGenerator();
//		setupMigrationTest();
//		setupSciDBExecution();
		setupParserTest();
	}
	
	public void setupQueryExplainer() {
		runExplainer = true;
	}; 
	
	public void setupQueryBuilder() {
		runBuilder = true;
	};
	
	public void setupRegexTester() {
		runRegex = true;
	};
	
	public void setupTreeWalker() {
		runWalker = true;
	};
	
	public void setupPlannerTester() {
		runPlanner = true;
	}
	
	public void setupMapTrial() {
		runMapTrial = true;
	}
	
	public void setupSchemaGenerator() {
		runSchemaGen = true;
	}
	
	public void setupMigrationTest() {
		runMigration = true;
	}
	
	public void setupSciDBExecution() {
		runSciDBExecution = true;
	}
	
	public void setupParserTest() {
		runParserTest = true;
	}
	
	@Test
	public void testRunExplainer() throws Exception {
		
		if ( !runExplainer ) return;
			
		PostgreSQLHandler psqlh = new PostgreSQLHandler(TheObjectThatResolvesAllDifferencesAmongTheIslands.psqlSchemaHandlerDBID);
//		System.out.println("Explainer -- Type query or \"quit\" to exit: ");
//		Scanner scanner = new Scanner(System.in);
//		String query = scanner.nextLine();
		String query = "SELECT s.bodc_station, s.longitude, s.latitude, s.bot_depth, count(m.bodc_station) AS sample_count FROM sampledata.station_info AS s, sampledata.main AS m WHERE s.bodc_station = m.bodc_station GROUP BY s.bodc_station";
		boolean started = false;
		while (!query.toLowerCase().equals("quit")) {
			
			SQLQueryPlan aqp = SQLPlanParser.extractDirectFromPostgreSQL(psqlh, query);
//			Select explainQuery = aqp.getStatement();
//			System.out.println(explainQuery + "\n");
			OperatorVisitor gen = new SQLQueryGenerator(); 
			aqp.getRootNode().accept(gen);
			System.out.println(gen.generateStatementString());
			
//			if (!started) {
//				query = "select * from region";
//				started = true;
//			} else 
				break;
		}
//		scanner.close();
	}

	@Test
	public void testRunBuilder() throws Exception {
		
		if ( !runBuilder ) return;
			
		SciDBHandler handler = new SciDBHandler(6);
		System.out.println("Builder -- Type query or \"quit\" to exit: ");
		Scanner scanner = new Scanner(System.in);
//		String query = scanner.nextLine();
//		String query = "select c_custkey, c_name from customer where c_custkey = 1 union select c_custkey as ckey, c_name from customer where c_custkey = 3 union all select c_custkey, c_name from customer where c_custkey = 5;";
//		String query = "select c1.c_custkey, c2.c_name from customer c2, customer c1 where c2.c_custkey = c1.c_custkey limit 3;";
		String query = "aggregate(region, approxDC(r_comment));";
		
//		String query = "select bucket, count(*) from ( select width_bucket(value1num, 0, 300, 300) as bucket from mimic2v26.chartevents ce,  mimic2v26.d_patients dp  where itemid in (6, 51, 455, 6701)  and ce.subject_id = dp.subject_id  and ((DATE_PART('year',ce.charttime) - DATE_PART('year',dp.dob))*12 + DATE_PART('month',ce.charttime) - DATE_PART('month',dp.dob)) > 15 ) as sbp group by bucket order by bucket;";
		while (!query.toLowerCase().equals("quit")) {
			
			AFLQueryPlan queryPlan = AFLPlanParser.extractDirect(handler, query);
			
			Operator root = queryPlan.getRootNode();
			
			OperatorVisitor gen = new AFLQueryGenerator();
			gen.configure(true, false);
			root.accept(gen);
			
			System.out.printf("Generated function: %s\n",gen.generateStatementString());
			
//			Select s = (Select)CCJSqlParserUtil.parse(query);
//			System.out.printf("Select body operation class: %s\n", ((SetOperationList)s.getSelectBody()).getOperations().get(0).getClass().getSimpleName());
			
			System.out.printf("Tree representation: %s\n",root.getTreeRepresentation(true));
			Signature.printO2EMapping(root);
			System.out.println();
			Signature.printStrippedO2EMapping(root);
			
//			System.out.println(RTED.computeDistance(root.getTreeRepresentation(true), "{}"));
			
			break;
//			query = scanner.nextLine();
			
		}
		scanner.close();
		
	}
	
	@Test
	public void testRegex() {
		
		if ( !runRegex ) return;
		
		String strippedQuery = "cross_join("
								+"	cross_join("
								+"		filter(region, region.r_name = 'AMERICA') as region_trimmed"
								+"		, nation as nation_trimmed"
								+"		, region_trimmed.r_regionkey, nation_trimmed.n_regionkey)"
								+"	, supplier as supplier_trimmed"
								+"	, nation_trimmed.n_nationkey, supplier_trimmed.s_nationkey)";

		System.out.println("Builder -- Type regexfinder or \"quit\" to exit: ");
		Scanner scanner = new Scanner(System.in);
		String regex = scanner.nextLine();
		while (!regex.toLowerCase().equals("quit")) {
			
//			Matcher m = Pattern.compile(regex).matcher(strippedQuery);
			
//			if (m.find())
				System.out.printf("-> {%s}\n", strippedQuery.replaceAll(regex, "")); //strippedQuery.substring(m.start(), m.end()));
//			else 
//				System.out.printf("-X Not found: %s;\n", regex);
			
			regex = scanner.nextLine();
		}
		scanner.close();
	}
	
	@Test
	public void testWalker() throws Exception {
		
		if ( !runWalker ) return;

		String query = "SELECT supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment FROM (SELECT partsupp_1.ps_partkey, min(partsupp_1.ps_supplycost) AS minsuppcost FROM nation AS nation_1, region AS region_1, supplier AS supplier_1, partsupp AS partsupp_1 WHERE (supplier_1.s_nationkey = nation_1.n_nationkey) AND (nation_1.n_regionkey = region_1.r_regionkey) AND (region_1.r_name = 'AMERICA') AND (partsupp_1.ps_suppkey = supplier_1.s_suppkey) GROUP BY partsupp_1.ps_partkey) AS BIGDAWGAGGREGATE_1, partsupp, part, supplier, nation, region WHERE ((BIGDAWGAGGREGATE_1.minsuppcost) = partsupp.ps_supplycost) AND (partsupp.ps_partkey = BIGDAWGAGGREGATE_1.ps_partkey) AND ((part.p_type LIKE '%BRASS') AND (part.p_size = 14)) AND (part.p_partkey = partsupp.ps_partkey) AND (part.p_partkey = partsupp.ps_partkey) AND (supplier.s_suppkey = partsupp.ps_suppkey) AND (nation.n_nationkey = supplier.s_nationkey) AND (region.r_name = 'AMERICA') AND (region.r_regionkey = nation.n_regionkey) AND (region.r_regionkey = nation.n_regionkey) ORDER BY supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey;";

		PostgreSQLHandler psqlh = new PostgreSQLHandler(3);
		SQLQueryPlan queryPlan = SQLPlanParser.extractDirectFromPostgreSQL(psqlh, query);
		SQLQueryGenerator gen = new SQLQueryGenerator();
		
		Operator root = queryPlan.getRootNode();
		root.accept(gen);
		System.out.println(gen.generateStatementString() + "\n");
		
		List<Operator> walker = new ArrayList<>();
		walker.add(root);
		while (!walker.isEmpty()) {
			List<Operator> nextgen = new ArrayList<>();
			
			for (Operator o: walker) {
				
				if (o instanceof Join) {
					System.out.println("Join encounter: ");
//					System.out.println("Printing: "+((Join)o).getJoinPredicateObjectsForBinaryExecutionNode());
					System.out.println();

				}
				
				
				nextgen.addAll(o.getChildren());
			}
			
			walker = nextgen;
		}
		
	}
	
	@Test
	public void testPlanner() throws Exception {
		if ( !runPlanner ) return;
		
//		String userinput = "bdrel(SELECT lineitem.l_orderkey, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue, orders.o_orderdate, orders.o_shippriority FROM orders, customer, lineitem WHERE (orders.o_custkey = customer.c_custkey) AND (orders.o_orderdate < '1996-01-02') AND (customer.c_mktsegment = 'AUTOMOBILE') AND (lineitem.l_shipdate > '1996-01-02') AND (lineitem.l_orderkey = orders.o_orderkey) GROUP BY lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority ORDER BY revenue DESC, orders.o_orderdate);";
//		String userinput = "bdrel(select c_custkey, c_name from customer where c_custkey = 1 union select c_custkey as ckey, c_name from customer where c_custkey = 3 union all select c_custkey, c_name from customer where c_custkey = 5);";
//		String userinput = "bdarray(cross_join(bdcast(bdrel(select * from region), reg, '<r_name:string>[r_regionkey=0:10,10,0]', array), bdcast(bdrel(select * from nation), nat, '<n_name:string>[n_regionkey=0:10,10,0,n_nationkey=0:1000,1000,0]', array), reg.r_regionkey, nat.n_regionkey));";
		String userinput = "bdarray(window(nation, 1, 0, 0, 0, count(n_name)));";
		try {
		Planner.processQuery(userinput, false);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	@Test
	public void testMap() throws Exception {
		if ( !runMapTrial ) return;
		
		Set<Integer> s1 = new HashSet<>();
		s1.add(2); s1.add(4); s1.add(6);  
		
		Map<Set<Integer>, String> m = new HashMap<>();
		m.put(s1, "246");
		
		Set<Integer> s2 = new HashSet<>();
		s2.add(2); s2.add(4); s2.add(6);
		
		Set<Integer> s3 = new HashSet<>();
		s3.add(2); s3.add(4);
		
		System.out.printf("get String with s1: %s; s2: %s; s3: %s\n", m.get(s1), m.get(s2), m.get(s3));
		
	}
	
	@Test
	public void testSchemaGeneration() throws Exception {
		if ( !runSchemaGen ) return;
		PostgreSQLHandler psqlh = new PostgreSQLHandler(1);
		
		Scanner scanner = new Scanner(System.in);
		String query = scanner.nextLine();

		while (!query.toLowerCase().equals("quit")) {
			
			System.out.println(psqlh.getCreateTable(query));
			
			
//			break;
			query = scanner.nextLine();
			
		}
		scanner.close();
		
	}
	
	@Test
	public void testMigration() throws Exception {
		if ( !runMigration ) return;
		
		ConnectionInfo ci1 = CatalogViewer.getConnectionInfo(4);
		ConnectionInfo ci2 = CatalogViewer.getConnectionInfo(6);
		
		Migrator.migrate(ci1, "orders", ci2, "ord", new MigrationParams("CREATE ARRAY ORD <o_custkey:int64>[o_orderkey=0:10,10,0]"));
		
	}
	
	@Test
	public void testSciDBExecution() throws Exception {
		if ( !runSciDBExecution ) return;
		
		ConnectionInfo ci = CatalogViewer.getConnectionInfo(6);
		Set<String> s = new HashSet<>();
		s.add("reg");
		s.add("nat");
		
		SciDBHandler h = new SciDBHandler(ci);
		Long time = System.currentTimeMillis();
		h.executeStatementAFL("remove(reg)");
		h.executeStatementAFL("remove(nat)");
//		h.commit();
		
		System.out.printf("Time it took: %s\n", System.currentTimeMillis() - time);
	}
	
	@Test
	public void testParserTest() throws BigDawgException {
		if ( !runParserTest) return;
			
		String input = "ExtractionRemote, 0.4, 110, 10, 1.010";
		
//		(new SStoreQueryParser()).parse(input);
			
			
	}
	
	
	
	
//	private void printIndentation(int recLevel) {
//		String token = "--";
//		for (int i = 0; i < recLevel; i++)
//			System.out.print(token);
//		System.out.print(' ');
//	}
	
}

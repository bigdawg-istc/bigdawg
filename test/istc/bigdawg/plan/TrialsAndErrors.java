package istc.bigdawg.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.generators.SQLQueryGenerator;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.planner.Planner;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.utils.sqlutil.SQLPrepareQuery;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;

public class TrialsAndErrors {
	
	private static boolean runExplainer = false;
	private static boolean runBuilder = false;
	private static boolean runRegex = false;
	private static boolean runWalker = false;
	private static boolean runPlanner = false;

	@Before
	public void setUp() throws Exception {
		CatalogInstance.INSTANCE.getCatalog();
		
//		setupQueryExplainer();
		setupQueryBuilder();
//		setupRegexTester();
//		setupTreeWalker();
//		setupPlannerTester();
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

	@Test
	public void testRunExplainer() throws Exception {
		
		if ( !runExplainer ) return;
			
		PostgreSQLHandler psqlh = new PostgreSQLHandler(3);
		System.out.println("Explainer -- Type query or \"quit\" to exit: ");
		Scanner scanner = new Scanner(System.in);
		String query = scanner.nextLine();
		while (!query.toLowerCase().equals("quit")) {
			
			String explainQuery = SQLPrepareQuery.generateExplainQueryString(query);
			System.out.println(psqlh.generatePostgreSQLQueryXML(explainQuery) + "\n");
			query = scanner.nextLine();
			
		}
		scanner.close();
	}

	@Test
	public void testRunBuilder() throws Exception {
		
		if ( !runBuilder ) return;
			
		PostgreSQLHandler psqlh = new PostgreSQLHandler(3);
		System.out.println("Builder -- Type query or \"quit\" to exit: ");
		Scanner scanner = new Scanner(System.in);
//		String query = scanner.nextLine();
		String query = "select c_custkey, c_name from customer where c_custkey = 1 union select c_custkey as ckey, c_name from customer where c_custkey = 3 union all select c_custkey, c_name from customer where c_custkey = 5;";
//		String query = "select c1.c_custkey, c2.c_name from customer c2, customer c1 where c2.c_custkey = c1.c_custkey limit 3;";
		
//		String query = "select bucket, count(*) from ( select width_bucket(value1num, 0, 300, 300) as bucket from mimic2v26.chartevents ce,  mimic2v26.d_patients dp  where itemid in (6, 51, 455, 6701)  and ce.subject_id = dp.subject_id  and ((DATE_PART('year',ce.charttime) - DATE_PART('year',dp.dob))*12 + DATE_PART('month',ce.charttime) - DATE_PART('month',dp.dob)) > 15 ) as sbp group by bucket order by bucket;";
		while (!query.toLowerCase().equals("quit")) {
			
			SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, query);
			
			Operator root = queryPlan.getRootNode();
			
			SQLQueryGenerator gen = new SQLQueryGenerator();
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
		
		String s = "width_bucket(valuenum, 0, 100, 1001)";
		
		StringBuilder sb = new StringBuilder();
		sb.append(s);

		Pattern pDayInterval = Pattern.compile("[(]");
		Matcher m3 = pDayInterval.matcher(sb);
		
		if (m3.find()) {
			System.out.println("--> INTERVAL: "+sb.substring(m3.start(), m3.end()));
		}
		
		
		s = s.replaceAll("[(]", "\\[\\(\\]"); 
//		Pattern p = Pattern.compile("'[0-9]+'");
//		Matcher m = p.matcher(s);
//		while (m.find()) {
//			s = m.replaceFirst(s.substring(m.start()+1, m.end()-1));
//			m.reset(s);
//		}
		
		System.out.println(s);
	}
	
	@Test
	public void testWalker() throws Exception {
		
		if ( !runWalker ) return;

		String query = "SELECT supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment FROM (SELECT partsupp_1.ps_partkey, min(partsupp_1.ps_supplycost) AS minsuppcost FROM nation AS nation_1, region AS region_1, supplier AS supplier_1, partsupp AS partsupp_1 WHERE (supplier_1.s_nationkey = nation_1.n_nationkey) AND (nation_1.n_regionkey = region_1.r_regionkey) AND (region_1.r_name = 'AMERICA') AND (partsupp_1.ps_suppkey = supplier_1.s_suppkey) GROUP BY partsupp_1.ps_partkey) AS BIGDAWGAGGREGATE_1, partsupp, part, supplier, nation, region WHERE ((BIGDAWGAGGREGATE_1.minsuppcost) = partsupp.ps_supplycost) AND (partsupp.ps_partkey = BIGDAWGAGGREGATE_1.ps_partkey) AND ((part.p_type LIKE '%BRASS') AND (part.p_size = 14)) AND (part.p_partkey = partsupp.ps_partkey) AND (part.p_partkey = partsupp.ps_partkey) AND (supplier.s_suppkey = partsupp.ps_suppkey) AND (nation.n_nationkey = supplier.s_nationkey) AND (region.r_name = 'AMERICA') AND (region.r_regionkey = nation.n_regionkey) AND (region.r_regionkey = nation.n_regionkey) ORDER BY supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey;";

		PostgreSQLHandler psqlh = new PostgreSQLHandler(3);
		SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, query);
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
		
		String userinput = "bdrel(SELECT lineitem.l_orderkey, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue, orders.o_orderdate, orders.o_shippriority FROM orders, customer, lineitem WHERE (orders.o_custkey = customer.c_custkey) AND (orders.o_orderdate < '1996-01-02') AND (customer.c_mktsegment = 'AUTOMOBILE') AND (lineitem.l_shipdate > '1996-01-02') AND (lineitem.l_orderkey = orders.o_orderkey) GROUP BY lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority ORDER BY revenue DESC, orders.o_orderdate);";
		try {
		Planner.processQuery(userinput, false);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public void printIndentation(int recLevel) {
		String token = "--";
		for (int i = 0; i < recLevel; i++)
			System.out.print(token);
		System.out.print(' ');
	}
	
}

package istc.bigdawg.plan;

import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import istc.bigdawg.utils.sqlutil.SQLPrepareQuery;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

public class TrialsAndErrors {
	
	private static boolean runExplainer = false;
	private static boolean runBuilder = false;
	private static boolean runRegex = false;
	private static boolean runWalker = false;

	@Before
	public void setUp() throws Exception {
		CatalogInstance.INSTANCE.getCatalog();
		
//		setupQueryExplainer();
		setupQueryBuilder();
//		setupRegexTester();
//		setupTreeWalker();
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
		String query = scanner.nextLine();
//		String query = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '1' day group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;";
		
//		String query = "SELECT lineitem.l_orderkey, sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue, orders.o_orderdate, orders.o_shippriority FROM orders, customer, lineitem WHERE (orders.o_custkey = customer.c_custkey) AND (orders.o_orderdate < '1996-01-02') AND (customer.c_mktsegment = 'AUTOMOBILE') AND (lineitem.l_shipdate > '1996-01-02') AND (lineitem.l_orderkey = orders.o_orderkey) GROUP BY lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority ORDER BY revenue DESC, orders.o_orderdate;";
		while (!query.toLowerCase().equals("quit")) {
			
			SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, query);
			System.out.println(queryPlan.getRootNode().generateSQLString(null) + "\n");
			
			System.out.println(queryPlan.getRootNode().getTreeRepresentation(true) + "\n");
			
			Signature.printO2EMapping(queryPlan.getRootNode());
			
			System.out.println();
			
			Signature.printStrippedO2EMapping(queryPlan.getRootNode());
			
//			System.out.println(RTED.computeDistance(queryPlan.getRootNode().getTreeRepresentation(true), "{}"));
			
//			break;
			query = scanner.nextLine();
			
		}
		scanner.close();
		
	}
	
	@Test
	public void testRegex() {
		
		if ( !runRegex ) return;
		
		String s = "where l_shipdate <= dAte '1998-12-01' - interval '1' day group by";
		
		StringBuilder sb = new StringBuilder();
		sb.append(s);

		Pattern pDayInterval = Pattern.compile("(?i)(interval '[0-9]+\\s?((hour)|(hours)|(day)|(days)|(month)|(months))?'(\\s((hour)|(hours)|(day)|(days)|(month)|(months)))?)");
		Matcher m3 = pDayInterval.matcher(sb);
		
		if (m3.find()) {
			System.out.println("--> INTERVAL: "+sb.substring(m3.start(), m3.end()));
		}
		
		
		s = s.replaceAll("::\\w+( \\w+)*", ""); 
		Pattern p = Pattern.compile("'[0-9]+'");
		Matcher m = p.matcher(s);
		while (m.find()) {
			s = m.replaceFirst(s.substring(m.start()+1, m.end()-1));
			m.reset(s);
		}
		
		System.out.println(s);
	}
	
	@Test
	public void testWalker() throws Exception {
		
		if ( !runWalker ) return;
		
//		PostgreSQLHandler psqlh = new PostgreSQLHandler(3);
//		System.out.println("Walker -- Type query or \"quit\" to exit: ");
//		Scanner scanner = new Scanner(System.in);
//		String query = scanner.nextLine();
//		
//		Expression e = null;
//		Select sel = null;
//		
//		scanner.close();
		
		String query = "exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)";
		Expression e = CCJSqlParserUtil.parseCondExpression(query);
		
		System.out.println(e.getClass()+"; "+e.toString());
	}
	
	public void printIndentation(int recLevel) {
		String token = "--";
		for (int i = 0; i < recLevel; i++)
			System.out.print(token);
		System.out.print(' ');
	}
	
}

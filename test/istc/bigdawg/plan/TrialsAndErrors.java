package istc.bigdawg.plan;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import convenience.RTED;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.utils.sqlutil.SQLExpressionUtils;
import istc.bigdawg.utils.sqlutil.SQLPrepareQuery;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

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
//		String query = "select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#13'  and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 30 and l_quantity <= 30 + 10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#55' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 10 and l_quantity <= 10 + 10 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#11' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 40 and l_quantity <= 40 + 10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' );";
		while (!query.toLowerCase().equals("quit")) {
			
			SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, query);
			System.out.println(queryPlan.getRootNode().generateSQLString(null) + "\n");
			
			System.out.println(queryPlan.getRootNode().getTreeRepresentation(true) + "\n");
			
			System.out.println(RTED.computeDistance(queryPlan.getRootNode().getTreeRepresentation(true), "{{}{}}"));
			
			query = scanner.nextLine();
			
		}
		scanner.close();
		
	}
	
	@Test
	public void testRegex() {
		
		if ( !runRegex ) return;
		
		String s = "where l_shipdate <= dAte '1998-12-01' - interval '1' day group by";
//		String s = "(lineitem.l_shipdate &lt;= '1998-11-30 00:00:00'::timestamp without time zone)";
//		System.out.println(s.replaceAll("::\\w+( \\w+)*", ""));
		
		StringBuilder sb = new StringBuilder();
		sb.append(s);
		
		Pattern p2 = Pattern.compile("(?i)(date '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')");
		Matcher m2 = p2.matcher(s);
		if (m2.find()) {
			sb.replace(m2.start(), m2.end(), "{d"+s.substring(m2.start()+4, m2.end())+"}");
			System.out.println("--> DATE: "+sb.toString());
		}
		
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

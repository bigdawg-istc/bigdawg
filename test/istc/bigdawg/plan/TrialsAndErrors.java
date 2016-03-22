package istc.bigdawg.plan;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.utils.sqlutil.SQLPrepareQuery;

public class TrialsAndErrors {
	
	private static boolean runExplainer = false;
	private static boolean runBuilder = false;
	private static boolean runRegex = false;

	@Before
	public void setUp() throws Exception {
		CatalogInstance.INSTANCE.getCatalog();
		
//		setupQueryExplainer();
		setupQueryBuilder();
//		setupRegexTester();
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
		while (!query.toLowerCase().equals("quit")) {
			
			SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, query);
			System.out.println(queryPlan.getRootNode().generateSQLString(null) + "\n");
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
}

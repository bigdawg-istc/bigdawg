package istc.bigdawg.islands.relational.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;




public class SQLPrepareQuery {

	static int xmlCounter = 0; // TODO CREATE A CLEARNER TO DELETE ALL THESE TEMP FILES
	private static Pattern pdate = Pattern.compile("(?i)(date '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')");
	private static Pattern pinterval = Pattern.compile("(?i)(date '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' +[+] +interval '[0-9]+' ((days)|(day)|(months)|(month)|(years)|(year)))");
	
	private static int defaultSchemaServerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	
	public static String readSQL(String filename) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
	    String sql = "";
	    String line;
	    while ((line = br.readLine()) != null) {
	    	
	    	line = line.replaceAll("\\-\\-.*$", ""); // delete any comments
	        sql = sql + line + " ";
	    }
	    br.close();
	    sql = sql.replaceAll("`", "");

	    return sql;
	}
	

	public static String generateExplainQueryString(String query) throws IOException {
		return "EXPLAIN (VERBOSE ON, COSTS OFF, FORMAT XML) " + query;
	}
	
	public static String generateExplainQueryStringWithPerformance(String query) throws IOException {
		return "EXPLAIN (VERBOSE ON, ANALYZE, FORMAT XML) " + query;
	}
	
	public static String generateSimpleExplainQueryString(String query) throws IOException {
		return "EXPLAIN (COSTS OFF, FORMAT XML) " + query;
	}
	
	public static String preprocessDateAndTime(String query) throws Exception {
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(query.toLowerCase());
		
		
		Matcher minterval = pinterval.matcher(sb);
		while (minterval.find()) {
			PostgreSQLHandler psqlh = new PostgreSQLHandler(defaultSchemaServerDBID);
			sb.replace(minterval.start(), minterval.end(), "'"+psqlh.computeDateArithmetic(sb.substring(minterval.start(), minterval.end()))+"'");
//			sb.replace(mdate.start(), mdate.end(), "{d"+sb.substring(mdate.start()+4, mdate.end())+"}");
			minterval.reset(sb);
		}
		
		Matcher mdate = pdate.matcher(query);
		while (mdate.find()) {
			sb.replace(mdate.start(), mdate.end(), sb.substring(mdate.start()+4, mdate.end()));
			mdate.reset(sb);
		}
		
		
		
		
		return sb.toString();
	}
	
	private static String generateExplainFile(String srcFilename) throws IOException {
		String[] pathTokens = srcFilename.split("\\/");
		String filename = "explain_" + pathTokens[pathTokens.length-1];
		String[] pathArray = Arrays.copyOf(pathTokens, pathTokens.length - 1);
		String path = StringUtils.join(pathArray, '/');

		String explainFilename =  path + "/" + filename;
		
		
		PrintWriter explain = new PrintWriter(explainFilename, "UTF-8");
		explain.println("EXPLAIN (VERBOSE ON, COSTS OFF, FORMAT XML)");
		explain.println(readSQL(srcFilename));
		explain.close();
		
		return explainFilename;
	}
	
	
}

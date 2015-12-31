package teddy.bigdawg.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import teddy.bigdawg.schema.SQLDatabaseSingleton;
import teddy.bigdawg.schema.SQLDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;




public class SQLPrepareQuery {

	static int xmlCounter = 0; // TODO CREATE A CLEARNER TO DELETE ALL THESE TEMP FILES
	
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
	
	public static void generatePlan(String srcFilename, String dstFilename) throws Exception {
		
		
		String explainFilename = generateExplainFile(srcFilename);
		

		SQLDatabaseSingleton d = SQLDatabaseSingleton.getInstance();
		SQLDatabase sd = d.getDatabase();
		String dbName = sd.getName();
		
		String path = SQLUtilities.getSMCQLRoot();
		
		
		String[] cmd = new String[] {path + "/scripts/explain.sh", dbName, explainFilename, dstFilename};
		
		Process p = java.lang.Runtime.getRuntime().exec(cmd);
		p.waitFor();

		
		
		int retVal = p.exitValue();
		assert(retVal == 0);
		
	}
	
	public static String generatePlanDirect(String path, String query) throws Exception {
		
		
		String explainQueryName = generateExplainQueryString(query);
		String xmlFileName = path + "explain_generated_xml_"+xmlCounter+".xml";
		xmlCounter += 1;
		
		SQLDatabaseSingleton d = SQLDatabaseSingleton.getInstance();
		SQLDatabase sd = d.getDatabase();
		String dbName = sd.getName();
		
		String shPath = SQLUtilities.getSMCQLRoot();
		
		
		String[] cmd = new String[] {shPath + "/scripts/explainDirect.sh", dbName, "5431", explainQueryName, xmlFileName};
		
		Process p = java.lang.Runtime.getRuntime().exec(cmd);
		p.waitFor();
		
		
		int retVal = p.exitValue();
		assert(retVal == 0);
		
		return xmlFileName;
	}
	
}

package istc.bigdawg.utils.sqlutil;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import istc.bigdawg.schema.SQLDatabaseSingleton;
import istc.bigdawg.schema.SQLAttribute;
import istc.bigdawg.schema.SQLDatabase;

public class SQLUtilities {

	// extract expression from XML string
	
	public static String parseString(String src) {
		
		if(src == null) {
			return src;
		}
		
		String dst = new String(src);
		
		
		dst = dst.replaceAll("&lt;&gt;", "!=");
		//dst = dst.replaceAll("<>", "!=");
		dst = dst.replaceAll("&lt;", "<");
		dst = dst.replaceAll("&gt;", ">");
		dst = dst.replaceAll("::text", "");
		
		dst = dst.replaceAll("~~", "like");
		
		dst = dst.replaceAll("::integer", "");
		dst = dst.replaceAll("\\[\\]", "");
		dst = dst.replaceAll("\\'\\{", "");  // remove psql optimizer notation
		dst = dst.replaceAll("\\}\\'", "");
		dst = dst.replaceAll(" = ANY", " IN"); // convert to form that JSQLParser accepts
		
		
		
		// flip order of time INTERVAL 
		dst = dst.replaceAll("(\\'.*?\\')::interval", "INTERVAL $1");
				
		// delete parens around column names - another psql-ism
		dst = dst.replaceAll("\\(([a-zA-Z0-9]+)\\)", "$1");
		dst = dst.replaceAll("\\(([a-zA-Z0-9]+\\.[a-zA-Z0-9]+)\\)", "$1");
		
		
		
		return dst;
		
	}
	

	public static String removeOuterParens(String src) {
		String dst = new String(src);  
		if(dst.startsWith("(") && dst.endsWith(")")) {
			  dst = dst.substring(1, dst.length()-1);
		  }
		
		return dst;
		

	}
	
	
	public static String indent(int tabCount) {
		String s = new String();
		for(int i = 0; i < tabCount; ++i) {
			s = s + '\t';
		}
		return s;
	}
	
	public static List<String> readFile(String filename) throws IOException  {				  
		{

			List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
			return lines;
				
		}
	}
	
	public static SQLAttribute lookUpAttribute(String table, String attribute) throws Exception {
		SQLDatabase d = SQLDatabaseSingleton.getInstance().getDatabase();
		SQLAttribute s = d.getTable(table).getAttribute(attribute);
		return s;
	}
	
	// look up smcql working directory
	public static String getSMCQLRoot() {
		URL location = SQLUtilities.class.getProtectionDomain().getCodeSource().getLocation();
		String path = location.getFile();
		
		// chop off trailing "/bin/"
		path = path.substring(0, path.length()-5);
		return path;
	}
	
	public static boolean isDBRef(String s) {
		return ((s.charAt(0) ^ 0x30) <= 0x9);
	}
}

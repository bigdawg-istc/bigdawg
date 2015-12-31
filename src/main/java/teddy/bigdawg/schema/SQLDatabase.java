package teddy.bigdawg.schema;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;


// create a list of JSQLParser tables from a schema file
// use this for type management in smc code
public class SQLDatabase {

	// list of tables and their column definitions
	private LinkedHashMap<String, SQLTable> tables;
	private String name;
	
	protected SQLDatabase() {  }
	
	
	// ddl consists of a set of create table statements
	protected SQLDatabase(String dbName, String ddl) throws Exception {
		
		name = dbName;
		
		tables = new LinkedHashMap<String, SQLTable>();
		CCJSqlParserManager pm = new CCJSqlParserManager();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(ddl)));
	    String sql = "";
	    String line;
	    while ((line = br.readLine()) != null) {
	        sql = sql + line;
	    }
	    br.close();
	    sql = sql.replaceAll("`", ""); 
	    String[] tableDefs =  sql.split(";");
	    
	    for (String tableDef : tableDefs) {
	    	Statement statement = pm.parse(new StringReader(tableDef));
	    	if (statement instanceof CreateTable) {
	    		CreateTable create = (CreateTable) statement;
	    		SQLTable t = new SQLTable(create); 
	    		tables.put(t.getName(), t);
	    	}
	    }

	}
	
	public String getName() {
		return name;
	}
	
	public SQLTable getTable(String tableName) {
		return tables.get(tableName);
	}
		
	public int getTableCount() {
		return tables.size();
	}
	
	
	public Iterator<Entry<String, SQLTable>> getTableIterator() {
		return  tables.entrySet().iterator();
		
	}
}

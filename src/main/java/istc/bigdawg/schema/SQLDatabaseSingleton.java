package istc.bigdawg.schema;

// object for reference to "main" schema
public class SQLDatabaseSingleton  {
	 private static SQLDatabaseSingleton instance = null;
	 private istc.bigdawg.schema.SQLDatabase db = null; 
	 
	   protected SQLDatabaseSingleton() throws Exception  {
			db = new SQLDatabase("bigdawg_schemas", "src/main/resources/schemas/plain.sql");
	   }

	   public static SQLDatabaseSingleton getInstance() throws Exception {
		      if(instance == null) {
		         instance = new SQLDatabaseSingleton();
		      }
		   
		      return instance;
		   }
	   

	   public void setDatabase(String name, String path) throws Exception {
		   db = new SQLDatabase(name, path);
	   }
	   
	   
	   public SQLDatabase getDatabase() {
		   return db;
	   }
	   
}

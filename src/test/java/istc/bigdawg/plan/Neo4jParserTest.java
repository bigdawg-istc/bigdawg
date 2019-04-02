package istc.bigdawg.plan;

import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.jdbc.Neo4jConnection;

import istc.bigdawg.postgresql.PostgreSQLHandler;


public class Neo4jParserTest {

	private static Logger log = Logger.getLogger(Neo4jParserTest.class.getName());
	
	Statement stmt;
	ResultSet rs;
	
	@Before
	public void setUp() throws Exception {
		// Make sure Neo4j Driver is registered
		Class.forName("org.neo4j.jdbc.Driver");
		
		// Connect
		Properties info = new Properties();
		info.put("user", "neo4j");
		info.put("password", "123");
//		final String connectionStr = "jdbc:neo4j://192.168.1.22:7474/";
		final String connectionStr = "jdbc:neo4j://localhost:7474/";
		Neo4jConnection con = (Neo4jConnection)DriverManager.getConnection(connectionStr, info);

		// Querying
		stmt = con.createStatement();
		
		
	}

	@Test
	public void test() {
		try {
			rs = stmt.executeQuery("MATCH (n)-[r]-() RETURN n, r limit 25");
			
			while(rs.next()) {
		        System.out.println(rs.getString("n")+" "+rs.getString("r"));
		    }
			
		} catch (SQLException e) {
			log.debug("FAIL:\n"+e.getMessage());
			fail();
		}
	    
	}

	
	@After
	public void done (){
		try {
			rs.close();
		} catch (SQLException e) {
			log.debug("Exception thrown during teardown:\n"+e.getMessage());
		}
	}
}

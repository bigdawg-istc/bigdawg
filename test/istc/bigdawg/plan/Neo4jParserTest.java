package istc.bigdawg.plan;

import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.jdbc.Neo4jConnection;


public class Neo4jParserTest {

	@Before
	public void setUp() throws Exception {
		// Make sure Neo4j Driver is registered
		Class.forName("org.neo4j.jdbc.Driver");
		
		// Connect
		Properties info = new Properties();
		info.put("user", "neo4j");
		info.put("password", "123");
		Neo4jConnection con = (Neo4jConnection)DriverManager.getConnection("jdbc:neo4j://192.168.1.22:7474/", info);

		// Querying
		Statement stmt = con.createStatement();
		
	    ResultSet rs = stmt.executeQuery("MATCH (n)-[r]-() RETURN n, r limit 25");
	    while(rs.next()) {
	        System.out.println(rs.getString("n")+" "+rs.getString("r"));
	    }
		
	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

}

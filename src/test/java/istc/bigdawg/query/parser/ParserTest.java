package istc.bigdawg.query.parser;

import static org.junit.Assert.assertEquals;
import istc.bigdawg.BDConstants.Island;
import istc.bigdawg.BDConstants.Operator;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.query.ASTNode;

import org.junit.Before;
import org.junit.Test;

public class ParserTest {
    @Before
    public void setUp() throws Exception {
    	
    }
    
    
    /**
     * Test to see that the message "Got it!" is sent in the response.
     */
    @Test
    public void testSingleRelational() {
        String query = "RELATION(select * from tableX)";
        ASTNode expected = new ASTNode("select * from tableX", Island.RELATION, Shim.PSQLRELATION, Operator.SCOPE);
        ASTNode expected2 = new ASTNode("select * from tableX", Island.RELATION, Shim.PSQLRELATION, Operator.SCOPE);
        
        Parser parser = null; //TODO add parser IMPL
        
        //DUMMY test
        assertEquals(expected, expected2);
        
        //TODO real test
        ASTNode parsed = parser.parseQueryIntoTree(query);
        assertEquals(expected, parsed);
    }
}

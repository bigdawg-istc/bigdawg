package istc.bigdawg.query.parser;

import static org.junit.Assert.assertEquals;
import istc.bigdawg.BDConstants.Operator;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.Island;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.query.ASTNode;

import org.junit.Before;
import org.junit.Test;

public class ParserTest {
    @Before
    public void setUp() throws Exception {
    	
    }
    
    
    /**
     * Test to see that the simple single node relation is selected
     * @throws NotSupportIslandException 
     */
    @Test
    public void testSingleRelational() throws NotSupportIslandException {
        String query = "RELATION(select * from tableX)";
        ASTNode expected = new ASTNode("select * from tableX", Island.RELATION, Shim.PSQLRELATION, Operator.SCOPE);
        ASTNode expected2 = new ASTNode("select * from tableX", Island.RELATION, Shim.PSQLRELATION, Operator.SCOPE);
        
        Parser parser =new simpleParser(); //TODO add parser IMPL
        
        ASTNode parsed = parser.parseQueryIntoTree(query);
        assertEquals(expected.getIsland(), parsed.getIsland());
        assertEquals(expected.getShim(),parsed.getShim());
        assertEquals(expected.getTarget(),parsed.getTarget());
    }
    
    /**
     * Test to see that the simple single node relation is selected with nested parens
     * @throws NotSupportIslandException 
     */
    @Test
    public void testSingleRelationalNestedParens() throws NotSupportIslandException {
        String query = "RELATION(select min(age) from tableX)";
        ASTNode expected = new ASTNode("select min(age) from tableX", Island.RELATION, Shim.PSQLRELATION, Operator.SCOPE);
        
        Parser parser =new simpleParser(); //TODO add parser IMPL
                
        ASTNode parsed = parser.parseQueryIntoTree(query);
        assertEquals(expected.getIsland(), parsed.getIsland());
        assertEquals(expected.getShim(),parsed.getShim());
        assertEquals(expected.getTarget(),parsed.getTarget());
    }
    
    
    /**
     * Test to see that the simple single node relation is selected with nested parens
     * @throws NotSupportIslandException 
     */
    @Test
    public void testSingleRelationalNestedParens2() throws NotSupportIslandException {
        String query = "RELATION(select min(age), avg(salary) from tableX)";
        ASTNode expected = new ASTNode("select min(age), avg(salary) from tableX", Island.RELATION, Shim.PSQLRELATION, Operator.SCOPE);
        
        Parser parser =new simpleParser(); //TODO add parser IMPL
                
        ASTNode parsed = parser.parseQueryIntoTree(query);
        assertEquals(expected.getIsland(), parsed.getIsland());
        assertEquals(expected.getShim(),parsed.getShim());
        assertEquals(expected.getTarget(),parsed.getTarget());
    }
}

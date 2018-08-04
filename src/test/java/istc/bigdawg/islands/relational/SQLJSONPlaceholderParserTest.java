package istc.bigdawg.islands.relational;

import istc.bigdawg.exceptions.QueryParsingException;
import org.junit.Test;

import static org.junit.Assert.*;

public class SQLJSONPlaceholderParserTest {

    @Test
    public void testContainsPlaceholder() {
        assertFalse(SQLJSONPlaceholderParser.containsPlaceholder(""));
        assertFalse(SQLJSONPlaceholderParser.containsPlaceholder("asdf"));
        assertFalse(SQLJSONPlaceholderParser.containsPlaceholder("select * from something"));
        assertFalse(SQLJSONPlaceholderParser.containsPlaceholder("select result from something"));
        assertTrue(SQLJSONPlaceholderParser.containsPlaceholder("select BIGDAWG_PLACEHOLDER from something"));
    }

    @Test
    public void testPossiblyContainsOperator() {
        assertTrue(SQLJSONPlaceholderParser.possiblyContainsOperator("select result -> 'asdf' from something"));
        assertTrue(SQLJSONPlaceholderParser.possiblyContainsOperator("select result ->> 'asdf' from something"));
        assertTrue(SQLJSONPlaceholderParser.possiblyContainsOperator("select a::json#>'{c,d}' from from something"));
        assertTrue(SQLJSONPlaceholderParser.possiblyContainsOperator("select a::json#>>'{c,d}' from from something"));
    }

    @Test
    public void testTransformJSONQuery() {
        try {
            SQLJSONPlaceholderParser.resetIndex();
            String result = SQLJSONPlaceholderParser.transformJSONQuery("select result -> 'asdf' from something");
            assertEquals("not equal", "select 'BIGDAWG_PLACEHOLDER1' from something", result);
            assertTrue(SQLJSONPlaceholderParser.containsPlaceholder(result));

            SQLJSONPlaceholderParser.resetIndex();
            result = SQLJSONPlaceholderParser.transformJSONQuery("select result -> 'asdf' -> 'asdf' from something");
            assertEquals("not equal", "select 'BIGDAWG_PLACEHOLDER2' from something", result);
            assertTrue(SQLJSONPlaceholderParser.containsPlaceholder(result));

            SQLJSONPlaceholderParser.resetIndex();
            result = SQLJSONPlaceholderParser.transformJSONQuery("select (result -> 'asdf') -> 'asdf' from something");
            assertEquals("not equal", "select 'BIGDAWG_PLACEHOLDER2' from something", result);

            SQLJSONPlaceholderParser.resetIndex();
            result = SQLJSONPlaceholderParser.transformJSONQuery("select (result -> 'asdf') ->> 'asdf' from something");
            assertEquals("not equal", "select 'BIGDAWG_PLACEHOLDER2' from something", result);

            SQLJSONPlaceholderParser.resetIndex();
            result = SQLJSONPlaceholderParser.transformJSONQuery("select count(visibility), coord ->> 'lon' from tab4 group by coord ->> 'lon'");
            assertEquals("not equal", "select count( visibility ) , 'BIGDAWG_PLACEHOLDER1' from tab4 group by 'BIGDAWG_PLACEHOLDER2'", result);
        }
        catch (QueryParsingException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testSubstitutePlaceholders() {
        try {
            SQLJSONPlaceholderParser.resetIndex();
            String sql = "select result -> 'asdf' from something";
            String result = SQLJSONPlaceholderParser.transformJSONQuery(sql);
            assertEquals("not equal", sql, SQLJSONPlaceholderParser.substitutePlaceholders(result));

            SQLJSONPlaceholderParser.resetIndex();
            sql = "select result -> 'asdf' -> 'asdf' from something";
            result = SQLJSONPlaceholderParser.transformJSONQuery(sql);
            assertEquals("not equal", sql, SQLJSONPlaceholderParser.substitutePlaceholders(result));

            SQLJSONPlaceholderParser.resetIndex();
            sql = "select (result -> 'asdf') -> 'asdf' from something";
            String expectedResult = "select ( result -> 'asdf' ) -> 'asdf' from something";
            result = SQLJSONPlaceholderParser.transformJSONQuery(sql);
            assertEquals("not equal", expectedResult, SQLJSONPlaceholderParser.substitutePlaceholders(result));

            SQLJSONPlaceholderParser.resetIndex();
            sql = "select (result -> 'asdf') ->> 'asdf' from something";
            expectedResult = "select ( result -> 'asdf' ) ->> 'asdf' from something";
            result = SQLJSONPlaceholderParser.transformJSONQuery(sql);
            assertEquals("not equal", expectedResult, SQLJSONPlaceholderParser.substitutePlaceholders(result));
        }
        catch (QueryParsingException e) {
            fail(e.getMessage());
        }
    }




}

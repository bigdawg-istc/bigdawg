package istc.bigdawg.accumulo;

import static org.junit.Assert.fail;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.islands.text.AccumuloJSONQueryParser;

public class AccumuloJSONQueryParserTest {

	JSONParser parser;
	AccumuloJSONQueryParser aparser;
	
	@Before
	public void setUp() throws Exception {
		parser = new JSONParser();
		aparser = new AccumuloJSONQueryParser();
	}

//	@Test
	public void testBasic() {
		String s ="{ 'op' : 'aggregate', "
				+ "    'sub'  : { 'op' : 'scan', "
				+ "               'table' : 'testtable' }, "
				+ "    'aggs' : { 'agg' : 'sum' }, "
				+ "    'gb'   : [ {'cf' : 'cf1', "
				+ "                'cq' : 'cq1', "
				+ "                'ts' : 'day'} ] }";
		JSONObject parsedObject;
		try {
			parsedObject = (JSONObject) parser.parse(s.replaceAll("[']", "\""));
			System.out.printf("String print of parsedObject: %s\n\n", parsedObject.get("op"));
		} catch (ParseException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testTextScanPrintOut() {
		String s ="{ 'op' : 'scan', "
				+ "  'table' : 'testtable', "
				+ "  'range' : { 'end' : ['S0100', '', '']} }";
		JSONObject parsedObject;
		try {
			String input = s.replaceAll("[']", "\"");
			parsedObject = (JSONObject) parser.parse(input);
			System.out.printf("Operator: %s, table: %s, range: [%s, %s] \n", parsedObject.get("op"), parsedObject.get("table"), 
					((JSONObject)parsedObject.get("range")).get("start"), ((JSONObject)parsedObject.get("range")).get("end"));
			System.out.printf("TextScan tree expression: %s\n\n", aparser.parse(input).getTreeRepresentation(true));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}

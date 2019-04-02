package istc.bigdawg.islands.text;


import istc.bigdawg.query.AbstractJSONQueryParser;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.text.operators.TextScan;

public class AccumuloJSONQueryParser extends AbstractJSONQueryParser {

	public OperatorTypes getOperatorType(JSONObject parsedObject, String input) throws ParseException {
		if (parsedObject.get("op") == null || !(parsedObject.get("op") instanceof String))
			throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "AccumuloJSONQueryParser parsing error: cannot identify operator. query: " + input);
		if (!parsedObject.get("op").equals("scan")) {
			throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, this.getClass() + " parsing error: unsupported operator: " + parsedObject.get("op"));
		}
		return OperatorTypes.SCAN;
	}

	protected Operator getScan(JSONObject parsedObject) throws ParseException {
		
		String table = (String)getObjectByType(parsedObject.get("table"), String.class);
		JSONObject rangeObject = (JSONObject)getObjectByType(parsedObject.get("range"), JSONObject.class);
		
		Range range = null;
		if (rangeObject != null) {
			JSONArray startArray = (JSONArray)getObjectByType(rangeObject.get("start"), JSONArray.class);
			JSONArray endArray = (JSONArray)getObjectByType(rangeObject.get("end"), JSONArray.class);
			
			Key start = null;
			Key end = null;
			
			if (startArray != null) {
				String row = getNonNullString(startArray.get(0));
				String cf  = getNonNullString(startArray.get(1));
				String cq  = getNonNullString(startArray.get(2));
				
				start = new Key( row, cf, cq); 
			}
			if (endArray != null) {
				String row = getNonNullString(endArray.get(0));
				String cf  = getNonNullString(endArray.get(1));
				String cq  = getNonNullString(endArray.get(2));
				
				end = new Key( row, cf, cq); 
			}
			range = new Range(start, end);
		} else {
			range = new Range();
		}
		
		return new TextScan(table, range);
	} 
}

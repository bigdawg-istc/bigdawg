package istc.bigdawg.islands.text;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.text.operators.TextScan;

public class AccumuloJSONQueryParser {

	JSONParser parser;
	
	public AccumuloJSONQueryParser() {
		parser = new JSONParser();
	}
	
	public Operator parse(String input) throws ParseException {
		JSONObject parsedObject = (JSONObject) parser.parse(input.replaceAll("[']", "\""));
		
		if (parsedObject.get("op") == null || !(parsedObject.get("op") instanceof String)) 
			throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "AccumuloJSONQueryParser parsing error: cannot identify operator. query: "+input);
		
		String operatorName = (String) parsedObject.get("op");
		
		switch (operatorName) {
		case "scan":
			return getScan(parsedObject);
		default:
			throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "AccumuloJSONQueryParser parsing error: unsupported operator: "+operatorName);
		}
	};
	
	@SuppressWarnings("rawtypes")
	private Object getObjectByType(Object objectHolder, Class clazz) throws ParseException {
		if (objectHolder == null)
			return null;
		if (! (objectHolder.getClass().equals(clazz)))
			throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "AccumuloJSONQueryParser parsing error: data type mismatch: expecting "+clazz.getName()+"; received: "+objectHolder.getClass().getName());
		return objectHolder;
	}
	
	private String getNonNullString(Object input) throws ParseException {
		String s = (String) getObjectByType(input, String.class);
		if (s == null) return "";
		return s;
	}
	
	private Operator getScan(JSONObject parsedObject) throws ParseException {
		
		String table = (String)getObjectByType(parsedObject.get("table"), String.class);
		JSONObject rangeArray = (JSONObject)getObjectByType(parsedObject.get("range"), JSONObject.class);
		
		Range range = null;
		if (rangeArray != null) {
			JSONArray startArray = (JSONArray)getObjectByType(rangeArray.get("start"), JSONArray.class);
			JSONArray endArray = (JSONArray)getObjectByType(rangeArray.get("end"), JSONArray.class);
			
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

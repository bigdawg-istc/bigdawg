package istc.bigdawg.query;

import istc.bigdawg.islands.operators.Operator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;

abstract public class AbstractJSONQueryParser {

    protected enum OperatorTypes { SCAN };

    private JSONParser parser;

    public AbstractJSONQueryParser() {
        parser = new JSONParser();
    }

    public Operator parse(String input) throws ParseException {
        JSONObject parsedObject = (JSONObject) parser.parse(input.replaceAll("[']", "\""));

        OperatorTypes operatorType = getOperatorType(parsedObject, input);
        switch (operatorType) {
            case SCAN:
                return getScan(parsedObject);
            default:
                throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, this.getClass() + " parsing error: unknown operator");
        }
    }

    abstract protected OperatorTypes getOperatorType(JSONObject jsonObject, String input) throws ParseException;

    abstract protected Operator getScan(JSONObject jsonObject) throws ParseException;

    @SuppressWarnings("rawtypes")
    public static Object getObjectByType(Object objectHolder, Class clazz) throws ParseException {
        if (objectHolder == null)
            return null;
        if (! (objectHolder.getClass().equals(clazz)))
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "AbstractJSONQueryParser parsing error: data type mismatch: expecting "+clazz.getName()+"; received: "+objectHolder.getClass().getName());
        return objectHolder;
    }

    protected String getNonNullString(Object input) throws ParseException {
        String s = (String) getObjectByType(input, String.class);
        if (s == null) return "";
        return s;
    }

    public static void addNonNullStringToList(Object input, List<String> output) throws ParseException{
        String s = (String) getObjectByType(input, String.class);
        if (s != null) output.add(s);
    }


}

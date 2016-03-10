package istc.bigdawg.schema;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

public class DataObjectAttribute {

	protected String name = null;
	protected Set<DataObjectAttribute> sources = null; //  the provenance of each DataObjectAttribute, map to prevent duplicates
	protected DataObject srcDataObject = null;
	protected String typeString = null;
	protected boolean hidden = false;
	protected Expression expression = null;
	
	public DataObjectAttribute(String n) {
		name = new String(n);
		// sources left null;
		// srcDataObject left null;
	}
	
	public DataObjectAttribute(DataObject o, String n) throws JSQLParserException {
		this(n);
		srcDataObject = new DataObject(o);
	}
	
	public DataObjectAttribute(DataObjectAttribute sa) throws JSQLParserException {
		this.name = new String(sa.name);
		if (sa.srcDataObject != null)
			this.srcDataObject = new DataObject(sa.srcDataObject);
		
		
		if (sa.sources != null) {
			this.sources = new HashSet<>();
			for (DataObjectAttribute a : sa.sources) {
				this.sources.add(new DataObjectAttribute(a));
			}
		}
		
		if (sa.expression != null)
			this.expression = CCJSqlParserUtil.parseExpression(new String(sa.expression.toString()));
	}


	public DataObjectAttribute() {}

	
	public void copy(DataObjectAttribute r) throws JSQLParserException {
		this.name = r.name;
		this.srcDataObject = new DataObject(r.srcDataObject);
		this.hidden = r.hidden;
		this.typeString = new String(r.typeString);
		this.sources = null;
		if (r.expression != null)
			this.expression = CCJSqlParserUtil.parseExpression(new String(r.expression.toString()));
		
		addSourceAttribute(r);

	}
	
	public void addSourceAttribute(DataObjectAttribute s) {
		if(sources == null) {
			sources = new HashSet<DataObjectAttribute>();
		}
		
		if(s.getSourceAttributes() == null) {
			sources.add(s);
		}
		else {
			sources.addAll(s.getSourceAttributes());
		}
	}
	
	public Set<DataObjectAttribute> getSourceAttributes() {
		return sources;
	}
	
	public  String getName() {
		return name;
	}
	
	
	public void setName(String n) {
		name = n;
	}
	
	public DataObject getDataObject() {
		return srcDataObject;
	}
	
	public void setDataObject(DataObject o) {
		srcDataObject = o;
	}
	
	public String getFullyQualifiedName() {
		if(srcDataObject != null) {
			return srcDataObject.getFullyQualifiedName() + "." + name;
		}
		else return name;
	}
	
	public void setTypeString (String t) {
		typeString = new String(t);
	}
	
	public String getTypeString () {
		return typeString;
	}
	
	public boolean isHidden() {
		return hidden;
	}
	
	public void setHidden(boolean hidden) {
		this.hidden = hidden;
	}
	
	
	public String generateSQLTypedString() {
//		return name.replaceAll(".+\\.(?=[\\w]+$)", "___") + " " + convertTypeStringToSQLTyped();
		return name.replaceAll(".+\\.(?=[\\w]+$)", "") + " " + convertTypeStringToSQLTyped();
	}
	
	public String generateAFLTypeString() {
		
		char token = ':';
		if (isHidden())
			token = '=';
		
		return name.replaceAll(".+\\.(?=[\\w]+$)", "") + token + convertTypeStringToAFLTyped();
		
	}
	
	public String convertTypeStringToSQLTyped() {
		
		if (typeString == null || typeString.charAt(0) == '*' || (typeString.charAt(0) >= '0' && typeString.charAt(0) <= '9'))
			return "integer";
		
		String str = typeString.concat("     ").substring(0,5).toLowerCase();
		
		switch (str) {
		case "int32":
		case "int64":
			return "integer";
		case "string":
			return "varchar";
		case "float":
			return "double precision";
		case "bool ":
			return "boolean";
		default:
			return typeString;
		}
		
	}
	
	public String convertTypeStringToAFLTyped() {
		
		if (typeString == null) {
			System.out.println("Missing typeString: "+ this.name);
			return "int64";
		}
		
		if (typeString.charAt(0) == '*' || (typeString.charAt(0) >= '0' && typeString.charAt(0) <= '9'))
			return typeString;
		
		String str = typeString.concat("     ").substring(0,5).toLowerCase();
		
		switch (str) {
		
		case "varch":
			return "string";
		case "times":
			return "datetime";
		case "doubl":
			return "double";
		case "integ":
		case "bigin":
			return "int64";
		case "boole":
			return "bool";
		default:
			return typeString;
		}
	}
	
	public Expression getSQLExpression() {
		return expression;
	}
	
	public void setExpression(Expression expr) throws JSQLParserException {
		if (expr != null)
			this.expression= CCJSqlParserUtil.parseExpression(new String (expr.toString()));
	}
	
	public void setExpression(String exprString) throws JSQLParserException {
		if (exprString != null)
			this.expression= CCJSqlParserUtil.parseExpression(new String (exprString));
	}
	
	public String getExpressionString() throws Exception {
		return expression.toString();
	}
	
	
	
	@Override
	public String toString() {
		
		String ret = new String(this.getFullyQualifiedName() + ": ");
		
		if(typeString != null) {
			ret += "type: " +  typeString;
		}
		
		return ret;
		
	}
}

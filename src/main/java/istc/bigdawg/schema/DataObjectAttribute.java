package istc.bigdawg.schema;

import java.util.HashSet;
import java.util.Set;

public class DataObjectAttribute {

	protected String name = null;
	protected Set<DataObjectAttribute> sources = null; //  the provenance of each DataObjectAttribute, map to prevent duplicates
	protected DataObject srcDataObject = null;
	protected String typeString = null;
	protected boolean hidden = false;
	
	public DataObjectAttribute(String n) {
		name = new String(n);
		// sources left null;
		// srcDataObject left null;
	}
	
	public DataObjectAttribute(DataObject o, String n) {
		this(n);
		srcDataObject = new DataObject(o);
	}
	
	public DataObjectAttribute(DataObjectAttribute sa) {
		this.name = new String(sa.name);
		if (sa.srcDataObject != null)
			this.srcDataObject = new DataObject(sa.srcDataObject);
		
		
		if (sa.sources != null) {
			this.sources = new HashSet<>();
			for (DataObjectAttribute a : sa.sources) {
				this.sources.add(new DataObjectAttribute(a));
			}
		}
	}


	public DataObjectAttribute() {}

	
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
		
//		if (isHidden())
//			return name.replaceAll("\\.", "___") + '=' + convertTypeStringToAFLTyped();
//		else 
//			return name.replaceAll("\\.", "___") + ':' + convertTypeStringToAFLTyped();
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
	
	
	
	
	@Override
	public String toString() {
		
		String ret = new String(this.getFullyQualifiedName() + ": ");
		
		if(typeString != null) {
			ret += "type: " +  typeString;
		}
		
		return ret;
		
	}
}

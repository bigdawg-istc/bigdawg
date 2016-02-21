package istc.bigdawg.schema;

import java.util.HashSet;
import java.util.Set;

public class DataObjectAttribute {

	protected String name = null;
	protected Set<DataObjectAttribute> sources = null; //  the provenance of each DataObjectAttribute, map to prevent duplicates
	protected DataObject srcDataObject = null;
	protected String typeString = null;
	
	
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
	
	public String getTypeString (String t) {
		return typeString;
	}
}

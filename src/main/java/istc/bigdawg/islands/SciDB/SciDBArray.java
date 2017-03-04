package istc.bigdawg.islands.SciDB;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.JSQLParserException;

public class SciDBArray {
	private String database = null;
	private String schema = null;
	private String name = null;
	private LinkedHashMap<String, SciDBAttributeOrDimension> attributes;
	
	
	public SciDBArray(String e, String s, String n) {
		this.database = e;
		this.schema = s;
		this.name = n;
		this.attributes = new LinkedHashMap<String, SciDBAttributeOrDimension>();
	}
	
	public SciDBArray(String n) {
		this.name = n;
		this.attributes = new LinkedHashMap<String, SciDBAttributeOrDimension>();
	}
	
	public SciDBArray(SciDBArray o) throws JSQLParserException {
		if (o.database != null) this.database = new String(o.database);
		if (o.schema != null) this.schema = new String(o.schema);
		if (o.name != null) this.name = new String(o.name);
		
		this.attributes = new LinkedHashMap<String, SciDBAttributeOrDimension>();
		for (String s : o.attributes.keySet()) {
			this.attributes.put(s, new SciDBAttributeOrDimension(o.attributes.get(s)));
		}
	}
	
	public SciDBArray(SciDBParsedArray aArray) {
		name = aArray.getAlias();
		schema = aArray.getSchemaString();

		attributes = new LinkedHashMap<String, SciDBAttributeOrDimension>();
		
		for(String att : aArray.getAttributes().keySet()) {
			SciDBAttributeOrDimension sa = new SciDBAttributeOrDimension(att);
			sa.setTypeString(aArray.getAttributes().get(att));
			attributes.put(sa.getName(), sa);
		}
		
		for(String dims : aArray.getDimensions().keySet()) {
			SciDBAttributeOrDimension sa = new SciDBAttributeOrDimension(dims);
			sa.setTypeString(aArray.getDimensions().get(dims).toString());
			attributes.put(sa.getName(), sa);
		}
	}
	
	public SciDBArray(){
		attributes = new LinkedHashMap<>();
	}
	
	public String getFullyQualifiedName() {
		StringBuilder sb = new StringBuilder();
		if (database != null)
			sb.append(database);
		if (sb.length() > 0)
			sb.append('.');
		if (schema != null)
			sb.append(schema);
		if (sb.length() > 0)
			sb.append('.');
		return sb.append(name).toString();
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = new String(name);
	}
	
	public void addAttribute(SciDBAttributeOrDimension a) throws Exception {
		if(attributes.containsKey(a.getName())) {
			throw new Exception("Duplicate attribute name not permitted");
		}
		
		attributes.put(a.getName(), a);
	}
	
	public SciDBAttributeOrDimension getAttributes(String colname) {
    	return attributes.get(colname);
    }
	
	
	String getAttributeName(int idx) {
		Map.Entry<String, SciDBAttributeOrDimension> kv = getColAtIdx(idx);
		return kv.getValue().getName();
	}
	
	
	private Map.Entry<String, SciDBAttributeOrDimension> getColAtIdx(int idx) {
		Iterator<Entry<String, SciDBAttributeOrDimension>> itr = attributes.entrySet().iterator();
		
		for (int i = 1; i < idx; ++i) {
			itr.next();
		}

		return (Map.Entry<String, SciDBAttributeOrDimension>) itr.next();
	}

	public LinkedHashMap<String, SciDBAttributeOrDimension>  getAttributes() {
		return attributes;
	}
}

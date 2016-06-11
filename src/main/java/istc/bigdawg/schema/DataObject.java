package istc.bigdawg.schema;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import istc.bigdawg.islands.SciDB.SciDBArray;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class DataObject {
	private String database = null;
	private String schema = null;
	private String name = null;
	private LinkedHashMap<String, DataObjectAttribute> attributes;
	
	
	public DataObject(String e, String s, String n) {
		this.database = e;
		this.schema = s;
		this.name = n;
		this.attributes = new LinkedHashMap<String, DataObjectAttribute>();
	}
	
	public DataObject(String n) {
		this.name = n;
		this.attributes = new LinkedHashMap<String, DataObjectAttribute>();
	}
	
	public DataObject(DataObject o) throws JSQLParserException {
		if (o.database != null) this.database = new String(o.database);
		if (o.schema != null) this.schema = new String(o.schema);
		if (o.name != null) this.name = new String(o.name);
		
		this.attributes = new LinkedHashMap<String, DataObjectAttribute>();
		for (String s : o.attributes.keySet()) {
			this.attributes.put(s, new DataObjectAttribute(o.attributes.get(s)));
		}
	}
	
	public DataObject(CreateTable aTable) throws Exception {
		name = aTable.getTable().getName();
		schema = aTable.getTable().getSchemaName();

		attributes = new LinkedHashMap<String, DataObjectAttribute>();
		
		for(ColumnDefinition aCol : aTable.getColumnDefinitions()) {
			
			SQLAttribute sa = new SQLAttribute(aCol, name);
			attributes.put(sa.getName(), sa);
		}
	}
	
	public DataObject(SciDBArray aArray) throws Exception {
		name = aArray.getAlias();
		schema = aArray.getSchemaString();

		attributes = new LinkedHashMap<String, DataObjectAttribute>();
		
		for(String att : aArray.getAttributes().keySet()) {
			DataObjectAttribute sa = new DataObjectAttribute(att);
			sa.setTypeString(aArray.getAttributes().get(att));
			attributes.put(sa.getName(), sa);
		}
		
		for(String dims : aArray.getDimensions().keySet()) {
			DataObjectAttribute sa = new DataObjectAttribute(dims);
			sa.setTypeString(aArray.getDimensions().get(dims).toString());
			attributes.put(sa.getName(), sa);
		}
	}
	
	public DataObject(){
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
	
	public void addAttribute(DataObjectAttribute a) throws Exception {
		if(attributes.containsKey(a.getName())) {
			throw new Exception("Duplicate attribute name not permitted");
		}
		
		attributes.put(a.getName(), a);
	}
	
	public DataObjectAttribute getAttributes(String colname) {
    	return attributes.get(colname);
    }
	
	
	String getAttributeName(int idx) {
		Map.Entry<String, DataObjectAttribute> kv = getColAtIdx(idx);
		return kv.getValue().getName();
	}
	
	
	private Map.Entry<String, DataObjectAttribute> getColAtIdx(int idx) {
		Iterator<Entry<String, DataObjectAttribute>> itr = attributes.entrySet().iterator();
		
		for (int i = 1; i < idx; ++i) {
			itr.next();
		}

		return (Map.Entry<String, DataObjectAttribute>) itr.next();
	}

	public LinkedHashMap<String, DataObjectAttribute>  getAttributes() {
		return attributes;
	}
}

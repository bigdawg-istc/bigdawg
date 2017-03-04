package istc.bigdawg.islands.relational.utils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import istc.bigdawg.exceptions.QueryParsingException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class SQLTable {
	private String database = null;
	private String schema = null;
	private String name = null;
	private LinkedHashMap<String, SQLAttribute> attributes;
	
	
	public SQLTable(String e, String s, String n) {
		this.database = e;
		this.schema = s;
		this.name = n;
		this.attributes = new LinkedHashMap<String, SQLAttribute>();
	}
	
	public SQLTable(String n) {
		this.name = n;
		this.attributes = new LinkedHashMap<String, SQLAttribute>();
	}
	
	public SQLTable(SQLTable o) throws JSQLParserException {
		if (o.database != null) this.database = new String(o.database);
		if (o.schema != null) this.schema = new String(o.schema);
		if (o.name != null) this.name = new String(o.name);
		
		this.attributes = new LinkedHashMap<String, SQLAttribute>();
		for (String s : o.attributes.keySet()) {
			this.attributes.put(s, new SQLAttribute(o.attributes.get(s)));
		}
	}
	
	public SQLTable(CreateTable aTable) throws QueryParsingException{
		name = aTable.getTable().getName();
		schema = aTable.getTable().getSchemaName();

		attributes = new LinkedHashMap<String, SQLAttribute>();
		
		try {
			for(ColumnDefinition aCol : aTable.getColumnDefinitions()) {
				
				SQLAttribute sa = new SQLAttribute(aCol, name);
				attributes.put(sa.getName(), sa);
			}
		} catch (JSQLParserException e) {
			throw new QueryParsingException(e.getMessage(), e);
		}
		
	}
	
	public SQLTable(){
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
	
	public void addAttribute(SQLAttribute a) throws Exception {
		if(attributes.containsKey(a.getName())) {
			throw new Exception("Duplicate attribute name not permitted");
		}
		
		attributes.put(a.getName(), a);
	}
	
	public SQLAttribute getAttributes(String colname) {
    	return attributes.get(colname);
    }
	
	
	String getAttributeName(int idx) {
		Map.Entry<String, SQLAttribute> kv = getColAtIdx(idx);
		return kv.getValue().getName();
	}
	
	
	private Map.Entry<String, SQLAttribute> getColAtIdx(int idx) {
		Iterator<Entry<String, SQLAttribute>> itr = attributes.entrySet().iterator();
		
		for (int i = 1; i < idx; ++i) {
			itr.next();
		}

		return (Map.Entry<String, SQLAttribute>) itr.next();
	}

	public LinkedHashMap<String, SQLAttribute>  getAttributes() {
		return attributes;
	}
}

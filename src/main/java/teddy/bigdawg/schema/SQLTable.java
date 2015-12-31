package teddy.bigdawg.schema;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

// wrapper for jqlparser createtable

public class SQLTable {



	// determine attribute security by parsing the first few letters of name
    // if attr begins with pri_ = private, pro_ = protected, pub_ = public

	
	private CreateTable tableDefinition; // whole provenance
	private String name; // table name
	private String schema; // schema name
	private LinkedHashMap<String, SQLAttribute> attributes; // name, vals
//	private boolean isReplicated;  // does this appear on all data sources with records pertaining to it or not?
	// isReplicated e.g., demographics - if a person has an encounter at a hospital, then they have a demographics entry for that patient
	// covers all foreign key relationships within that engine
	// if tablename starts with r_ then replicated
	// if it starts with s_ then standalone
	
	// strip out all of these prefixes before ingesting schema

	private String alias;


	
	public SQLTable(CreateTable aTable) throws Exception {
		tableDefinition = aTable;
		alias = "";
		name = aTable.getTable().getName();
		schema = aTable.getTable().getSchemaName();

		attributes = new LinkedHashMap<String, SQLAttribute>();
		
		for(ColumnDefinition aCol : aTable.getColumnDefinitions()) {
			
			SQLAttribute sa = new SQLAttribute(aCol, name);
			attributes.put(sa.getName(), sa);
		}
		
		
	}
	
	public SQLTable() {
		name = "anonymous";
		schema = null;
		alias = null;
		attributes = new LinkedHashMap<String, SQLAttribute>();
	}
	
	public SQLTable(SQLTable src) {
		this.name = new String(src.name);
		this.schema = new String(src.schema);
		if(src.alias != null) {
			alias = new String(src.alias);
		}
		attributes = new LinkedHashMap<String, SQLAttribute>(src.attributes);
	}
	
	public void setAlias(String a) {
		alias = a;
	}
	
	
	public void addAttribute(SQLAttribute a) throws Exception {
		if(attributes.containsKey(a.getName())) {
			throw new Exception("Duplicate attribute name not permitted");
		}
		
		attributes.put(a.getName(), a);
	}
	
	public String getName() {
		return name;
	}
	
	public String getSchemaName() {
		return schema;
	}
	
	public String getColumnSQLType(String colname) {
		return attributes.get(colname).toString();
	}
	
	
	// for varchar(X)
    public int getColumnStrLength(String colname) {
		ColDataType colType = attributes.get(colname).getType();
		String bracketed = colType.getArgumentsStringList().get(0); // e.g., [5]
		return Integer.parseInt(bracketed.substring(1, bracketed.length()-1));
		
	}
	
    public SQLAttribute getAttribute(String colname) {
    	return attributes.get(colname);
    }

    public SQLAttribute getFullyQualifiedAttribute(String colname) {
    	int nameStart = colname.indexOf('.') + 1; // remove any aliases
    	return attributes.get(colname.substring(nameStart));
    }

    
    
    
    String getColumnName(int idx) {
		Map.Entry<String, SQLAttribute> kv = getColAtIdx(idx);
		return kv.getKey();
	}
	
	public Set<String> getColumnNames() {
		return attributes.keySet();
	}
	
	
	ColDataType getColumnSQLType(int idx) {
		Map.Entry<String, SQLAttribute> kv = getColAtIdx(idx);
		return kv.getValue().getType();
		
	}
	
	
	String getColumnSQLTypeStr(int idx) {
		Map.Entry<String, SQLAttribute> kv = getColAtIdx(idx);
		return kv.getValue().getType().getDataType(); 
		
	}
	
//    public SQLAttribute.SecurityPolicy getAttributeSecurity(int idx) {
//		Map.Entry<String, SQLAttribute> kv = getColAtIdx(idx);
//		return kv.getValue().getSecurityPolicy(); 
//    }

	
    public SQLAttribute getAttribute(int idx) {
		Map.Entry<String, SQLAttribute> kv = getColAtIdx(idx);
		return kv.getValue();
    }

    public LinkedHashMap<String, SQLAttribute>  getAttributes() {
		return attributes;
	}
	
    
	private Map.Entry<String, SQLAttribute> getColAtIdx(int idx) {
		Iterator<Entry<String, SQLAttribute>> itr = attributes.entrySet().iterator();
		for(int i = 1; i < idx; ++i) {
			itr.next();
		}

		return (Map.Entry<String, SQLAttribute>) itr.next();

	}
	
	
	
		
}

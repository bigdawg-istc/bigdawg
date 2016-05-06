package istc.bigdawg.schema;

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

public class SQLStoredTable {



	// determine attribute security by parsing the first few letters of name
    // if attr begins with pri_ = private, pro_ = protected, pub_ = public

	
	private CreateTable tableDefinition; // whole provenance
	private String name; // table name
	private LinkedHashMap<String, SQLStoredAttribute> attributes; // name, vals
	private boolean isReplicated;  // does this appear on all data sources with records pertaining to it or not?
	// isReplicated e.g., demographics - if a person has an encounter at a hospital, then they have a demographics entry for that patient
	// covers all foreign key relationships within that engine
	// if tablename starts with r_ then replicated
	// if it starts with s_ then standalone
	
	// strip out all of these prefixes before ingesting schema



	
	public SQLStoredTable(CreateTable aTable) throws Exception {
		tableDefinition = aTable;
		name = aTable.getTable().getName();
		
		if(name.startsWith("r_")) {
			isReplicated = true;
		}
		else if (name.startsWith("s_")) {
			isReplicated = false;
		}
		else {
			throw new Exception("Failed to parse replication prefix " + name + " for table " + name + " must start with r_ or s_ for replicated or not");
		}
		
		name = name.substring(2); // strip out prefix

		attributes = new LinkedHashMap<String, SQLStoredAttribute>();
		
		for(ColumnDefinition aCol : aTable.getColumnDefinitions()) {
			
			SQLStoredAttribute sa = new SQLStoredAttribute(aCol, this, isReplicated);
			attributes.put(sa.getName(), sa);
		}
		
		
	}
	
	public SQLStoredTable() {
		name = "anonymous";
		attributes = new LinkedHashMap<String, SQLStoredAttribute>();
		isReplicated = false;
	}
	
	public SQLStoredTable(SQLStoredTable src) {
		this.name = new String(src.name);
		isReplicated = src.isReplicated;
		attributes = new LinkedHashMap<String, SQLStoredAttribute>(src.attributes);
	}
	
	
	
	public void addAttribute(SQLStoredAttribute a) throws Exception {
		if(attributes.containsKey(a.getName())) {
			throw new Exception("Duplicate attribute name not permitted");
		}
		
		attributes.put(a.getName(), a);
	}
	
	public String getName() {
		return name;
	}
	
	public String getColumnSQLType(String colname) {
		return attributes.get(colname).toString();
	}
	
	
//	public String getColumnSMCType(String colname) throws IOException {
//		String sql = attributes.get(colname).toString();
//		return TypeMap.getInstance().toSMC(sql);
//	}

	// for varchar(X)
    public int getColumnStrLength(String colname) {
		ColDataType colType = attributes.get(colname).getType();
		String bracketed = colType.getArgumentsStringList().get(0); // e.g., [5]
		return Integer.parseInt(bracketed.substring(1, bracketed.length()-1));
		
	}
	
    public SQLStoredAttribute.SecurityPolicy getAttributeSecurity(String colname) {
    	return attributes.get(colname).getSecurityPolicy();
    }
	
    public SQLStoredAttribute getAttribute(String colname) {
    	return attributes.get(colname);
    }


    
    
    
    String getColumnName(int idx) {
		Map.Entry<String, SQLStoredAttribute> kv = getColAtIdx(idx);
		return kv.getKey();
	}
	
	public Set<String> getColumnNames() {
		return attributes.keySet();
	}
	
	
	ColDataType getColumnSQLType(int idx) {
		Map.Entry<String, SQLStoredAttribute> kv = getColAtIdx(idx);
		return kv.getValue().getType();
		
	}
	
	
	String getColumnSQLTypeStr(int idx) {
		Map.Entry<String, SQLStoredAttribute> kv = getColAtIdx(idx);
		return kv.getValue().getType().getDataType(); 
		
	}
	
    public SQLStoredAttribute.SecurityPolicy getAttributeSecurity(int idx) {
		Map.Entry<String, SQLStoredAttribute> kv = getColAtIdx(idx);
		return kv.getValue().getSecurityPolicy(); 
    }

	
    public SQLStoredAttribute getAttribute(int idx) {
		Map.Entry<String, SQLStoredAttribute> kv = getColAtIdx(idx);
		return kv.getValue();
    }

    public LinkedHashMap<String, SQLStoredAttribute>  getAttributes() {
		return attributes;
	}
	
    
	private Map.Entry<String, SQLStoredAttribute> getColAtIdx(int idx) {
		Iterator<Entry<String, SQLStoredAttribute>> itr = attributes.entrySet().iterator();
		for(int i = 1; i < idx; ++i) {
			itr.next();
		}

		return (Map.Entry<String, SQLStoredAttribute>) itr.next();

	}
	
	
	
		
}

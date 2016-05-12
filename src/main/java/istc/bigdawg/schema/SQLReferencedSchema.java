package istc.bigdawg.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//for intermediate results
public class SQLReferencedSchema implements java.io.Serializable {

	private static final long serialVersionUID = -1098317191017520394L;
	private String tableName; 
	private String alias;
	Map<String, String> filters = null; // StoredTable name --> any filters

	private List<SQLReferencedAttribute> attributes; 

	public SQLReferencedSchema(String name, String alias) {
		tableName = name;
		this.alias = alias;
		attributes = new ArrayList<SQLReferencedAttribute>();
		filters = new HashMap<String, String>();
	}
	
	
	public SQLReferencedSchema(SQLReferencedSchema src) {
		filters = new HashMap<String, String>(src.filters);
		
		if(src.alias != null) {
			alias = new String(src.alias);
		}
		
		this.tableName = src.tableName;
		
		this.attributes = new ArrayList<SQLReferencedAttribute>();

		for(SQLReferencedAttribute r : src.attributes) {
			this.addAttribute(r);
		}

	}
	
	public SQLReferencedSchema() {
		attributes = new ArrayList<SQLReferencedAttribute>();
		filters = new HashMap<String, String>();

	}

	public SQLReferencedSchema(SQLStoredTable baseTable) {
		attributes = new ArrayList<SQLReferencedAttribute>();
		tableName = baseTable.getName();
		
		for(SQLStoredAttribute s : baseTable.getAttributes().values()) {
			SQLReferencedAttribute r = new SQLReferencedAttribute(s);
			addAttribute(r);
		}
		filters = new HashMap<String, String>();

	}

	public void setAlias(String a) {
		alias = a;
		
		for(SQLReferencedAttribute r : attributes) {
			r.setTableAlias(a);
		}
		
	}

	public boolean contains(SQLReferencedAttribute r) {
		return attributes.contains(r);
	}
	
	public void addAttribute(SQLReferencedAttribute r) {

		
		SQLReferencedSchema src = r.getParentResult();
		if(src != null) { // not a leaf node
			String filter = src.getFilter(r.getCTEName());
			if(filter != null) {
				this.addFilter(r.getCTEName(), filter);
			}
		}
		
		SQLReferencedAttribute rPrime = new SQLReferencedAttribute(r);
		rPrime.setParentResult(this);


		attributes.add(rPrime);
		

	}
	
	public void cloneFilters(SQLReferencedSchema c) {
		this.filters = new HashMap<String, String>(c.filters);
	}
	
	public void addFilters(SQLReferencedSchema c) {
		for(String s : c.filters.keySet()) {
			this.filters.put(s, c.filters.get(s));
		}
	}
	
	public List<SQLReferencedAttribute> getAttributes() {
		return attributes;
		
	}
	
	public String getName() {
		return tableName;
	}
	
	public SQLReferencedAttribute getAttribute(String name) {
		
		for(SQLReferencedAttribute r : attributes) {
			if(r.isCalled(name)) {
				return r;
			}
		}
		return null;
		
	}

	public String getTableAlias() {
		return alias;
	}
	
	public String toString() {
		String out = "Table: " + tableName + " alias: " + alias + "\nAttributes:\n";
		for(SQLReferencedAttribute r : attributes) {
			out += r.getFullyQualifiedName() + "\n";
		}
		return out;
	}

	public void setName(String name) {

		tableName = name;
	}

	// entering a new SELECT block
	public void resetAttributeAliases() {
		for(SQLReferencedAttribute r : attributes) {
			String a = r.getAlias();
			if(a != null) {
				r.setName(a);
				r.setAlias(null);
			}
		}
		
	}

	public SQLReferencedAttribute getAttribute(Integer i) {
		return attributes.get(i);
	}

	public String getFilter(String tableName) {
		String filter = filters.get(tableName);
		return filter;

	}

	public String getFilters() {
		return filters.toString();
	}
	
	public void addFilter(String table, String filter) {
		filters.put(table, filter);
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof SQLReferencedSchema) {
			SQLReferencedSchema rhs = (SQLReferencedSchema) o;
			if(!rhs.attributes.equals(this.attributes)) {
				return false;
			}
			if(this.tableName != null && rhs.tableName != null) {
				if(!this.tableName.equals(rhs.tableName)) {
					return false;
				}
			}
			return true;
		}
		
		return false;
	}

	
}

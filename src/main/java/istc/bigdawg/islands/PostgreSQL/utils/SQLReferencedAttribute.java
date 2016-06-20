package istc.bigdawg.islands.PostgreSQL.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class SQLReferencedAttribute extends SQLStoredAttribute  implements java.io.Serializable   {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5356339777481520704L;
	private String attrAlias = null; 
	protected Set<SQLStoredAttribute> sources = null; //  the provenance of each attribute, map to prevent duplicates
	private String cteName = null;
	private String SQLStoredTableName = null;
	private String tableAlias = null; // srcTable always equal to null here
	private SQLReferencedSchema parentResult = null;
	
	
	// possible states:
	// name + alias
	// expression + alias
	// name alone
	
	
	public SQLReferencedAttribute(SQLStoredAttribute s) {
		super(s);
		SQLStoredTableName = s.getTable().getName();
		tableAlias = null;
		
		sources = new HashSet<SQLStoredAttribute>();
		sources.add(s);
		
		this.srcTable = null;
		
	}
	
	
	public SelectExpressionItem getSelectItem() throws JSQLParserException { 
		
		String a = new String();
		if(tableAlias != null) {
			a += tableAlias + ".";
		}
		else if(cteName != null) {
			a+= cteName + ".";
		}
		else if(SQLStoredTableName != null) {
			a += SQLStoredTableName + ".";
		}
		
		a += attrName;
		
		
		Expression expr = CCJSqlParserUtil.parseExpression(a);
		SelectExpressionItem sei = new SelectExpressionItem(expr);
		
		if(attrAlias != null) {
			sei.setAlias(new Alias(attrAlias));
		}
		
		
		
		return sei;
	}
	public String prettyPrint() {
	
		String a = new String();
		if(tableAlias != null) {
			a += tableAlias + ".";
		}
		else if(cteName != null) {
			a+= cteName + ".";
		}
		else if(SQLStoredTableName != null) {
			a += SQLStoredTableName + ".";
		}
		
		if(attrAlias != null) {
			a += attrAlias;
		}
		else {
			a += attrName;
		}

		return a;
	}
	
	public String prettyPrintSMC() {
		return StringUtils.replace(prettyPrint(), ".", "_");
	}
	
	public SQLReferencedAttribute(SQLReferencedAttribute r) {
		super(r);
		this.attrAlias = r.attrAlias;
		this.sources = new HashSet<SQLStoredAttribute>(r.sources);
		this.cteName = r.cteName;
		this.SQLStoredTableName = r.SQLStoredTableName;
		
		if(r.getTableAlias() != null) {
			this.setTableAlias(new String(r.getTableAlias()));
		}
		
		this.srcTable = null;

	}
	
	public SQLReferencedAttribute() {
		super();
		this.srcTable = null;

	}


	public String toString() {
		String out = super.toString();
		out += " alias: " + attrAlias; 
		out += ", table: " + SQLStoredTableName + " " + cteName + " " + tableAlias; 
		if(parentResult != null) {
			out += " filter: " + parentResult.getFilter(this.getCTEName());
		}
		else {
			out += " No parent!";
		}
		
		return out;
	}
	
	@Override
	public boolean equals(Object o) {
		
		if(!(o instanceof SQLReferencedAttribute)) {
			return false;
		}
		
		SQLReferencedAttribute r  = (SQLReferencedAttribute) o;
		if(isCalled(r.getFullyQualifiedName())) {
			return true;
		}
		 if(r.isCalled(this.getFullyQualifiedName())) {
			 return true;
		 }
		 
		 return false;
	}
	
	
	public List<String> allNames() {
		List<String> names = new ArrayList<String>();
		
		List<String> attrNames = new ArrayList<String>();
		if(attrName != null) {
			attrNames.add(attrName);
			names.add(attrName);
		}
		
		if(attrAlias != null) {
			attrNames.add(attrAlias);
			names.add(attrAlias);
		}
		
		if(SQLStoredTableName != null) {
			for(String a : attrNames) {
				names.add(SQLStoredTableName + "." + a);
			}
			
		}
		
		if(cteName != null) {
			for(String a : attrNames) {
				names.add(cteName + "." + a);
			}
	
		}
		
		if(tableAlias != null) {
			for(String a : attrNames) {
				names.add(tableAlias + "." + a);
			}
	
		}
		
		return names;
	}

	

	// name is fully qualified if possible
	public boolean isCalled(String aName) {

		List<String> names = allNames();
		boolean ret = names.contains(aName) ? true : false;
		return ret;
	}


	public String getAlias() {
		return attrAlias;
	}


	public void setAlias(String alias) {
		this.attrAlias = alias;
	}
	

	public String getDeclaration() {
		String ret = srcTable.getName() + "." + attrName;
		if(attrAlias != null) {
			ret += " " + attrAlias;
		}
		return ret;
	}
	
	
	// maintain a list of table attributes from which this slice key is derived
	// all source attributes must appear in a SecureTable
	public void addSourceAttribute(SQLReferencedAttribute s) {
		if(sources == null) {
			sources = new HashSet<SQLStoredAttribute>();
		}
		
		if(s.getSourceAttributes() == null) {
			sources.add(s);

			updateSecurityPolicy(s);
		}
		else {
			sources.addAll(s.sources);
			for(SQLStoredAttribute src : s.sources) {
				updateSecurityPolicy(src);
			}
		}
	}
	
	public List<SQLStoredAttribute> getSourceAttributes() {
		return new ArrayList<SQLStoredAttribute>(sources);
	}
	
//	public String getBitmask() throws Exception {
//		return CodeGenUtils.getBitmask(parentResult, this);
//	}

	// if an attr is derived from more than one attribute, take maximum security policy
	 void updateSecurityPolicy(SQLStoredAttribute attr) {
		SecurityPolicy p = attr.getSecurityPolicy();

		if(p.compareTo(this.attributeSecurity) > 0) {
			this.attributeSecurity = p;
		}
	}

	public static SecurityPolicy maxPolicy(SecurityPolicy a, SecurityPolicy b) {
		if(a.compareTo(b) > 0) {
			return a;				
		}

		return b;
	}



	
	

	// deep copy
	public void copy(SQLReferencedAttribute r) {
		this.attrAlias = r.attrAlias;
		this.attributeSecurity = r.attributeSecurity;
		this.attrName = r.attrName;
		this.isReplicated = r.isReplicated;

		if(r.getCTEName() != null) {
			cteName = new String(r.getCTEName());
		}

		if(r.getTableAlias() != null) {
	 		this.setTableAlias(new String(r.getTableAlias()));
		}
		if(r.SQLStoredTableName != null) {
			this.setSQLStoredTableName(r.SQLStoredTableName);
		}
		
		this.srcTable = r.srcTable;
		this.type = r.type;
		
		addSourceAttribute(r);

	}


	public void setSourceTable(SQLStoredTable object) {
		srcTable = object;
		
	}

	public String getCTEName() {
		return cteName;
	}

	public void setSQLStoredTableName(String tableName) {
		SQLStoredTableName = tableName;
	}
	
	public String getSQLStoredTableName() {
		return SQLStoredTableName;
	}
	public void setCTEName(String tableName) {
		this.cteName = tableName;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	public void setTableAlias(String tableAlias) {
		this.tableAlias = tableAlias;
	}


	public int hashCode() {
		int hashCode = 1;
		
		if(cteName != null) {
			for(int i = 0; i < cteName.length(); ++i) {
				hashCode *= (int) cteName.charAt(i);
			}
		}
		
		for(int i = 0; i < attrName.length(); ++i) {
			hashCode *= (int) attrName.charAt(i);
		}
		
		return hashCode;

	}
	
	@Override
	public String getFullyQualifiedName() {
		String name = new String();
		
		if(tableAlias != null) {
			name = tableAlias + ".";
		}
		else if(cteName != null) {
			name = cteName + ".";
		}
		else if(SQLStoredTableName != null) {
			name = SQLStoredTableName + ".";
		}
		
		name += attrName;
		return name;
		
	}


	public void setParentResult(SQLReferencedSchema intermediateResult) {
		parentResult = intermediateResult;
		
	}
	
	public SQLReferencedSchema getParentResult() {
		return parentResult;
	}


	public void setAttrName(String a) {
		attrName = a;
	}
	
}

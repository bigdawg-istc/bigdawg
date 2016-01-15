package istc.bigdawg.schema;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;




public class SQLAttribute {
	

	// order used in type inference
	// needs to always be ordered lowest to most restrictive
	
	
		private ColDataType type;
		private String name;
		private Expression expr;  // for provenance of out columns in SELECT statements
		private Set<SQLAttribute> sources = null; //  the provenance of each attribute, map to prevent duplicates
		private String srcTable = null;
		
		SQLAttribute(ColDataType t, String n) {
			type = t;
			name = new String(n);
		}
		
		SQLAttribute(ColumnDefinition a, String table) throws Exception {
			
			
			name = a.getColumnName();
			type = a.getColDataType();
			srcTable = table;
			
			String typeName = type.getDataType();
			
			type.setDataType(typeName);
		}
		
		public SQLAttribute(SQLAttribute sa) {
			this.type = sa.type;
			this.name = new String(sa.name);
			if (sa.srcTable != null) this.srcTable = new String(sa.srcTable);
			this.expr = sa.expr;
			
			if (sa.sources != null) {
				this.sources = new HashSet<>();
				for (SQLAttribute a : sa.sources) {
					this.sources.add(new SQLAttribute(a));
				}
			}
		}


		public SQLAttribute() {
		}


		// maintain a list of table attributes from which this slice key is derived
		// all source attributes must appear in a SecureTable
		public void addSourceAttribute(SQLAttribute s) {
			if(sources == null) {
				sources = new HashSet<SQLAttribute>();
			}
			
			if(s.getSourceAttributes() == null) {
				sources.add(s);
			}
			else {
				sources.addAll(s.getSourceAttributes());
			}
		}
		
		public Set<SQLAttribute> getSourceAttributes() {
			return sources;
		}

		
		public  ColDataType getType() {
			return type;
		}
		
		public  String getName() {
			return name;
		}
		
		
		public void setName(String n) {
			name = n;
		}

		public void setType(ColDataType src) {
			type = src;
			
		}
		
		public void setExpression(Expression e) {
			expr = e;
		}
		
		public Expression getExpression() {
			return expr;
		}
		
		public String toString() {
			
			String ret = new String(this.getFullyQualifiedName() + ": ");
			
			if(type != null) {
				ret += "type: " +  type.getDataType();
			}
			
//			ret += " security: " + attributeSecurity;
			return ret;
			
		}
		
		
		// if an attr is derived from more than one attribute, take maximum security policy
//		public void updateSecurityPolicy(SQLAttribute attr) {
//			SecurityPolicy p = attr.attributeSecurity;
//			if(p.compareTo(this.attributeSecurity) > 0) {
//				this.attributeSecurity = p;
//			}
//		}

//		public static SecurityPolicy maxPolicy(SecurityPolicy a, SecurityPolicy b) {
//			if(a.compareTo(b) > 0) {
//				return a;				
//			}
//
//			return b;
//		}


		public String getFullyQualifiedName() {
			if(srcTable != null) {
				return srcTable + "." + name;
			}
			else return name;
		}
		
		public String getTable() {
			return srcTable;
		}
		
//		public String getStructEntry(int indentLevel) throws IOException {
//			TypeMap typeMap = TypeMap.getInstance();
//			String entry = SQLUtilities.indent(indentLevel) + typeMap.toSMC(type.toString()) + " " + this.getFullyQualifiedName().replace('.', '_') + ";";
//			return entry;
//		}
		
		
		

}

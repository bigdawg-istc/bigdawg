package istc.bigdawg.islands.relational.utils;



import java.util.HashSet;
import java.util.Set;

import istc.bigdawg.islands.SciDB.SciDBArray;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;




//public class SQLAttribute extends SQLAttribute {
public class SQLAttribute {
	

	// order used in type inference
	// needs to always be ordered lowest to most restrictive
		protected String name = null;
		protected Set<SQLAttribute> sources = null; //  the provenance of each SQLAttribute, map to prevent duplicates
		protected SciDBArray srcDataObject = null;
		protected String typeString = null;
		protected boolean hidden = false;
		protected String expression = null;
	
		private ColDataType type;
		
		SQLAttribute(ColDataType t, String n) {
			name = new String(n);
			type = t;
			typeString = type.getDataType();
		}
		
		public SQLAttribute(ColumnDefinition a, String table) throws JSQLParserException {
			
			name = new String(a.getColumnName());
			srcDataObject = new SciDBArray(table);
			
			type = a.getColDataType();
			String typeName = type.getDataType();
			type.setDataType(new String(typeName));
			typeString = type.getDataType();
			expression = a.getColumnName();
		}
		
		public SQLAttribute(SQLAttribute sa) throws JSQLParserException {
			this.name = new String(sa.name);
			if (sa.srcDataObject != null)
				this.srcDataObject = new SciDBArray(sa.srcDataObject);
			
			
			if (sa.sources != null) {
				this.sources = new HashSet<>();
				for (SQLAttribute a : sa.sources) {
					this.sources.add(new SQLAttribute(a));
				}
			}
			
			if (sa.expression != null)
				this.expression = new String(sa.expression.toString());
			
			this.type = sa.type;
			typeString = type.getDataType();
		}


		public SQLAttribute() {
		}

		
		public void addSourceAttribute(SQLAttribute s) {
			if(sources == null) {
				sources = new HashSet<>();
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
		
		public void setType(ColDataType src) {
			type = src;
			
		}
		
		public  String getName() {
			return name;
		}
		
		public void setName(String n) {
			name = n;
		}
		
		public void setTypeString (String t) {
			typeString = new String(t);
		}
		
		public String getTypeString () {
			return typeString;
		}
		
		public String toString() {
			String ret = new String(this.getFullyQualifiedName());
			if(type != null) ret += ":"+ type.getDataType();
			return ret;
		}
		
		public String getFullyQualifiedName() {
			if(srcDataObject != null) {
				return srcDataObject.getFullyQualifiedName() + "." + name;
			}
			else return name;
		}


		public Expression getSQLExpression() {
			try {
				return CCJSqlParserUtil.parseExpression(expression);
			} catch (JSQLParserException e) {
				return null;
			}
		}
		
		public void setExpression(Expression expr) {
			if (expr != null)
				this.expression= new String (expr.toString());
		}
		
		public void setExpression(String exprString) {
			if (exprString != null)
				this.expression= new String (exprString);
		}
		
		public String getExpressionString() {
			if (expression == null) return null;
			return expression.toString();
		}
		

}

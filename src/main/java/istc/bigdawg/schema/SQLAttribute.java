package istc.bigdawg.schema;



import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;




public class SQLAttribute extends DataObjectAttribute {
	

	// order used in type inference
	// needs to always be ordered lowest to most restrictive
	
	
		private ColDataType type;
		
		SQLAttribute(ColDataType t, String n) {
			super(n);
			type = t;
			typeString = type.getDataType();
		}
		
		SQLAttribute(ColumnDefinition a, String table) throws Exception {
			super(new DataObject(table), a.getColumnName());
			
			type = a.getColDataType();
			String typeName = type.getDataType();
			type.setDataType(new String(typeName));
			typeString = type.getDataType();
			
		}
		
		public SQLAttribute(SQLAttribute sa) throws JSQLParserException {
			super(sa);
			this.type = sa.type;
			typeString = type.getDataType();
		}


		public SQLAttribute() {
		}


		public  ColDataType getType() {
			return type;
		}
		
		public void setType(ColDataType src) {
			type = src;
			
		}
		
		public String toString() {
			
			String ret = new String(this.getFullyQualifiedName() + ": ");
			
			if(type != null) {
				ret += "type: " +  type.getDataType();
			}
			
			return ret;
			
		}
		
		


		
		

}

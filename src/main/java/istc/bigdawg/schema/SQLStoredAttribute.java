package istc.bigdawg.schema;



import java.io.IOException;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;




public class SQLStoredAttribute  implements java.io.Serializable  {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 8956772290694690567L;

	// order used in type inference
	// needs to always be ordered lowest to most restrictive
	public enum SecurityPolicy {
		Public, Protected, Private
	};
	
	
	protected SecurityPolicy attributeSecurity = null;
	protected ColDataType type;
	protected String attrName;
	protected boolean isReplicated;
	protected SQLStoredTable srcTable = null;
				
		public boolean getReplicated() {
			return isReplicated;
		}


		public void setReplicated(boolean isReplicated) {
			this.isReplicated = isReplicated;
		}


		SQLStoredAttribute(SecurityPolicy a, ColDataType t, String n, boolean r) {
			attributeSecurity = a;
			type = t;
			attrName = new String(n);
			isReplicated = r;  // for use within query plan
			
		}
		
		
		SQLStoredAttribute(ColumnDefinition a, SQLStoredTable table, boolean replicated) throws Exception {
			
			
			attrName = a.getColumnName();
			type = a.getColDataType();
			srcTable = table;
			
			String typeName = type.getDataType();
			isReplicated = replicated;
			
			attributeSecurity = SecurityPolicy.Public;
			
			if(typeName.startsWith("protected_")) {
				attributeSecurity = SecurityPolicy.Protected;
				typeName = typeName.replace("protected_", "");
			}
			else if(typeName.startsWith("private")) {
				attributeSecurity = SecurityPolicy.Private;
				typeName =  typeName.replace("private_", "");
		
			}
			
			type.setDataType(typeName);
		}
		
		public SQLStoredAttribute(SQLStoredAttribute sa) {
			attributeSecurity = sa.attributeSecurity;
			type = sa.type;
			if(sa.attrName != null) {
				attrName = new String(sa.attrName);
			}
			isReplicated = sa.isReplicated;  
		}


		public SQLStoredAttribute() {
		}


		public int hashCode() {
			int hashCode = 1;
			
			if(srcTable.getName() != null) {
				String tableName = srcTable.getName();
				for(int i = 0; i < tableName.length(); ++i) {
					hashCode *= (int) tableName.charAt(i);
				}
			}
			
			for(int i = 0; i < attrName.length(); ++i) {
				hashCode *= (int) attrName.charAt(i);
			}
			
			return hashCode;
		}

		
		public SecurityPolicy getSecurityPolicy() {
			return attributeSecurity;
		}
		
		public  ColDataType getType() {
			return type;
		}
		
		public  String getName() {
			return attrName;
		}
		
		
		public void setName(String n) {
			attrName = n;
		}
		
		public void setSecurityPolicy(SecurityPolicy a) {
			attributeSecurity = a;
		}


		public void setType(ColDataType src) {
			type = src;
			
		}
		
//		// size in bits
//		public int size() throws IOException {
//			TypeMap tmap = TypeMap.getInstance();
//			return tmap.sizeof(this);
//		}
		
		public String toString() {
			
			String ret = new String(this.getFullyQualifiedName() + ": ");
			
			if(type != null) {
				ret += "type: " +  type.toString();
			}
			
			
			ret += " security: " + attributeSecurity;
			return ret;
			
		}
		
		@Override
		public boolean equals(Object o) {

			if(!(o instanceof SQLStoredAttribute)) {
				return false;
			}
			
			SQLStoredAttribute s = (SQLStoredAttribute) o;
			
			if(this.getFullyQualifiedName().equals(s.getFullyQualifiedName())) {
				return true;
			}
			
			return false;
		}

		public String getFullyQualifiedName() {
			if(srcTable != null) {
				return srcTable.getName() + "." + attrName;
			}
			else return attrName;
		}
		
		public SQLStoredTable getTable() {
			return srcTable;
		}
		
		
		
		public String prettyPrint() {
			return getFullyQualifiedName();
		}
		
		public String prettyPrintSMC() {
			return getFullyQualifiedName().replace('.', '_');
		}
		
		

}

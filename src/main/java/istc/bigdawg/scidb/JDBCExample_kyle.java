/**
 * http://paradigm4.com/HTMLmanual/13.3/scidb_ug/ch11s05.html
 *
 * To run this example, make sure SciDB is running, then run the following commands:
 *  cd $JDBC
 *  wget http://downloads.paradigm4.com/client/13.3/jdbc/example.jar
 *  java -classpath example.jar:scidb4j.jar:/usr/share/java/protobuf.jar
 *  org.scidb.JDBCExample
 *  If the example runs without error, the output is similar to the following:
 *
 *  Source array name: build
 *  3 columns:
 *  x - int64 - is attribute:false
 *  y - int64 - is attribute:false
 *  a - string - is attribute:true
 *  =====
 *  x y a
 *  -----
 *  0 0 a
 *  0 1 b
 *  0 2 c
 *  1 0 d
 *  1 1 e
 *  1 2 f
 *  2 0 123
 *  2 1 456
 *  2 2 789
 *
 */


package istc.bigdawg.scidb;

import java.io.IOException;
import java.sql.*;

class JDBCExample_kyle {
	public static void main(String[] args) throws IOException {
		try {
			Class.forName("org.scidb.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Driver is not in the CLASSPATH -> " + e);
		}

		try {
			Connection conn = DriverManager.getConnection("jdbc:scidb://192.168.99.100/");
			conn.setAutoCommit(false);
			Statement st = conn.createStatement();
			ResultSet res = st.executeQuery("select * from *");
			ResultSetMetaData meta = res.getMetaData();

			System.out.println("Source array name: " + meta.getTableName(0));
			System.out.println(meta.getColumnCount() + " columns:");

			while (!res.isAfterLast()) {
				
				for (int i = 1 ; i <= meta.getColumnCount(); i++ ) {
					Object value = null;
	            	switch (meta.getColumnTypeName(i).toLowerCase()) {
	            		case "int64":
	            			value = res.getLong(i);
	            			break;
	            		case "string":
	            			value = res.getString(i);
	            			break;
	            		case "float":
	            			value = res.getFloat(i);
	            			break;
	            		case "double":
	            			value = res.getDouble(i);
	            			break;
	            		case "datetime":
	            			value = res.getDate(i);
	            			break;
	            		case "bool":
	            			value = res.getBoolean(i);
	            			break;
	            		default:
	            			throw new SQLException("SciDB JDBC result set row retrieval does not support type: "
									+ meta.getColumnTypeName(i).toLowerCase());
	            	}
	            	System.out.printf("%s ", value);
				}
				System.out.println();
				res.next();
			}
			res.close();
			st.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage().contains("does not exist"));
			System.out.println(e.getMessage());
			//System.out.println(e);
		}
		System.exit(0);
	}
}

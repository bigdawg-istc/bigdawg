package istc.bigdawg.scidb;

import org.scidb.jdbc.IResultSetWrapper;
import org.scidb.jdbc.IStatementWrapper;

import com.google.common.primitives.UnsignedLong;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

class JDBCExample {
	public static void main(String[] args) throws IOException {
		try {
			Class.forName("org.scidb.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Driver is not in the CLASSPATH -> " + e);
		}

		try {
			Connection conn = DriverManager.getConnection("jdbc:scidb://codd7/");
			Statement st = conn.createStatement();
			
//			boolean result = st.execute("load region from '/home/adam/data/tpch/tpch1G/csv/region.sci'");
//			boolean result = st.execute("select * from t1");
//			System.out.println("result of the load statement: "+result);
//			ResultSet resLoad = st.getResultSet();
//			// create array A<a:string>[x=0:2,3,0, y=0:2,3,0];
//			// select * into A from
//			// array(A, '[["a","b","c"]["d","e","f"]["123","456","789"]]');
//			ResultSet res = st.executeQuery("select * from " + " array(<a:string>[x=0:2,3,0, y=0:2,3,0],"
//					+ " '[[\"a\",\"b\",\"c\"][\"d\",\"e\",\"f\"][\"123\",\"456\",\"789\"]]')");
			
			// this unwraps to afl
			IStatementWrapper stWrapper = st.unwrap(IStatementWrapper.class);
			stWrapper.setAfl(true);
			
//			ResultSet res = st.executeQuery("explain_logical('cross_join(project(filter(poe_med, i <= 5), poe_id, drug_type, dose_val_disp) AS a, project(filter(poe_med, i <= 3), poe_id, dose_val_rx) AS b, a.i, b.i)', 'afl')");
			ResultSet res = st.executeQuery("explain_logical('cross_join(project(filter(poe_med, i <= 5), poe_id, drug_type, dose_val_disp) AS a, project(filter(redimension(poe_med, <drug_type:string,drug_name:string,drug_name_generic:string,prod_strength:string,form_rx:string,dose_val_rx:string,dose_unit_rx:string,form_val_disp:string,form_unit_disp:string,dose_val_disp:double,dose_unit_disp:string,dose_range_override:string>[poe_id=0:10000000,1,1]), poe_id = 3750047), dose_val_rx) AS b)', 'afl')");
			ResultSetMetaData meta = res.getMetaData();

			System.out.println("Source array name: " + meta.getTableName(0));
			System.out.println(meta.getColumnCount() + " columns:");

			IResultSetWrapper resWrapper = res.unwrap(IResultSetWrapper.class);
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				System.out.println(meta.getColumnName(i) + " - " + meta.getColumnTypeName(i) + " - is attribute:"
						+ resWrapper.isColumnAttribute(i));
			}
			
//			System.out.println("=====");
//
//			System.out.println("x y a");
//			System.out.println("-----");
			while (!res.isAfterLast()) {
				System.out.println(res.getString("logical_plan"));//.getLong("x") + " " + res.getLong("y") + " " + res.getString("a"));
				res.next();
			}
		} catch (SQLException e) {
			System.out.println(e);
		}
		System.exit(0);
	}
}

package istc.bigdawg.sstore;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCExample {

    public static void main(String[] args) {
	// TODO Auto-generated method stub
	try {
	    Class.forName("org.voltdb.jdbc.Driver");
	} catch (ClassNotFoundException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	}
	Connection conn = null;
	Statement stmt1 = null;
	PreparedStatement stmt2 = null;
	try {

	    conn = DriverManager.getConnection("jdbc:voltdb://node-051:21212");
	    String sql1 = "Select * from sflavg_tbl;";
	    stmt1 = conn.createStatement();
	    ResultSet results1 = stmt1.executeQuery(sql1);
	    System.out.println("query 1 result: ");
	    while (results1.next()) {
		System.out.println(results1.getInt(1));
	    }
/*
	    String sql2 = "Select * from contestants where contestant_number = ?";
	    stmt2 = conn.prepareStatement(sql2);
	    stmt2.setInt(1, 10);
	    ResultSet results2 = stmt2.executeQuery();
	    System.out.println("query 2 result: ");
	    while (results2.next()) {
		System.out.println(results2.getString(2));
	    }

	    DatabaseMetaData metaData = conn.getMetaData();
	    ResultSet result3 = metaData.getColumns(null, null, "CONTESTANTS", null);
	    System.out.println("query 3 result: ");
	    while (result3.next()) {
		System.out.println(result3.getString("COLUMN_NAME"));
	    }

	    ResultSet result4 = metaData.getColumns(null, null, "contestants", null);
	    System.out.println("query 4 result: ");
	    while (result4.next()) {
		System.out.println(result4.getString("COLUMN_NAME") + " " + result4.getString("TYPE_NAME") + "   "
			+ result4.getBoolean("IS_NULLABLE") + " " + result4.getInt("RELATIVE_INDEX") + " "
			+ result4.getInt("COLUMN_SIZE"));
	    }
*/
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} finally {
	    try {
		if (stmt1 != null) {
		    stmt1.close();
		}
		if (stmt2 != null) {
		    stmt2.close();
		}
	    } catch (SQLException e) {

	    }
	    try {
		if (conn != null) {
		    conn.close();
		}
	    } catch (SQLException e) {
		e.printStackTrace();
	    }
	    System.out.print("GOODBYE!");
	}

    }
}

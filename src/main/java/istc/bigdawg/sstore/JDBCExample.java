package istc.bigdawg.sstore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCExample {
    
    public static void main(String[] args) {
	try {
	    Class.forName("org.voltdb.jdbc.Driver");
	} catch (ClassNotFoundException e1) {
	    e1.printStackTrace();
	}
	Connection conn = null;
	Statement stmt1 = null;
	PreparedStatement stmt2 = null;
	Statement stmt3 = null;
	try {
	    conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212", "test", "password");
	    String sql1 = "Select * from area_code_state limit 1;";
	    stmt1 = conn.createStatement();
	    ResultSet results1 = stmt1.executeQuery(sql1);
	    ResultSetMetaData resultSetMetaData1 = results1.getMetaData();
	    for (int i = 1; i <= resultSetMetaData1.getColumnCount(); i++) {
//		System.out.println(resultSetMetaData1.get);
		System.out.println(resultSetMetaData1.getColumnLabel(i));
		System.out.println(resultSetMetaData1.getColumnName(i));
		System.out.println(resultSetMetaData1.getColumnType(i));
		System.out.println(resultSetMetaData1.getColumnTypeName(i));
		System.out.println(resultSetMetaData1.getPrecision(i));
		System.out.println(resultSetMetaData1.getScale(i));
		System.out.println(resultSetMetaData1.getSchemaName(i));
//		System.out.println(resultSetMetaData1.getColumnDisplaySize(i));
		
	    }
	    System.out.println("query 1 result: ");
//	    while (results1.next()) {
//		System.out.println(results1.getInt(1));
//	    }
//	    
	    String sql3 = "INSERT INTO contestants (contestant_name, contestant_number) VALUES ('Shabo Tian', 666);";
	    stmt3 = conn.createStatement();
	    stmt3.execute(sql3);

	    String sql2 = "Select * from contestants where contestant_number = ?";
	    stmt2 = conn.prepareStatement(sql2);
	    stmt2.setInt(1, 666);
	    ResultSet results2 = stmt2.executeQuery();
	    System.out.println("query 2 result: ");
	    while (results2.next()) {
		System.out.println(results2.getString(2));
	    }
	    
//	    String sql4 = "select colume_name, data_type, character_maximum_length fro"
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
		if (stmt3 != null) {
		    stmt3.close();
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

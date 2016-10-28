package istc.bigdawg.scidb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.scidb.jdbc.IResultSetWrapper;

class JDBCExample
{
	public static void main(String [] args) throws IOException
	{
		try
		{
			Class.forName("org.scidb.jdbc.Driver");
		}
		catch (ClassNotFoundException e)
		{
			System.out.println("Driver is not in the CLASSPATH -> " + e);
		}

		try
		{
			Connection conn = DriverManager.getConnection("jdbc:scidb://192.168.99.100/");
			Statement st = conn.createStatement();
			// create array A<a:string>[x=0:2,3,0, y=0:2,3,0];
			// select * into A from
			// array(A, '[["a","b","c"]["d","e","f"]["123","456","789"]]');
			ResultSet res = st.executeQuery("select * from array(<a:string>[x=0:2,3,0, y=0:2,3,0],'[[\"a\",\"b\",\"c\"][\"d\",\"e\",\"f\"][\"123\",\"456\",\"789\"]]')");
			ResultSetMetaData meta = res.getMetaData();

			System.out.println("Source array name: " + meta.getTableName(0));
			System.out.println(meta.getColumnCount() + " columns:");

			IResultSetWrapper resWrapper =
					res.unwrap(IResultSetWrapper.class);
			for (int i = 1; i <= meta.getColumnCount(); i++)
			{
				System.out.println(meta.getColumnName(i) + " - " + meta.getColumnTypeName(i)
						+ " - is attribute:" + resWrapper.isColumnAttribute(i));
			}
			System.out.println("=====");

			System.out.println("x y a");
			System.out.println("-----");
			while(!res.isAfterLast())
			{
				System.out.println(res.getLong("x") + " " + res.getLong("y") + " "
						+ res.getString("a"));
				res.next();
			}
		}
		catch (SQLException e)
		{
			System.out.println(e);
		}
		System.exit(0);
	}
}

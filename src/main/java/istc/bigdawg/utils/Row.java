/**
 * 
 */
package istc.bigdawg.utils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.glassfish.grizzly.utils.StringEncoder;

/**
 * @author Adam Dziedzic
 * 
 */
public class Row {
	public List<Entry<Object, Class>> row;
	public static Map<String, Class> TYPE;

	static {
		TYPE = new HashMap<String, Class>();

		TYPE.put("INTEGER", Integer.class);
		TYPE.put("TINYINT", Byte.class);
		TYPE.put("SMALLINT", Short.class);
		TYPE.put("BIGINT", Long.class);
		TYPE.put("REAL", Float.class);
		TYPE.put("FLOAT", Double.class);
		TYPE.put("DOUBLE", Double.class);
		TYPE.put("DECIMAL", BigDecimal.class);
		TYPE.put("NUMERIC", BigDecimal.class);
		TYPE.put("BOOLEAN", Boolean.class);
		TYPE.put("CHAR", String.class);
		TYPE.put("VARCHAR", String.class);
		TYPE.put("LONGVARCHAR", String.class);
		TYPE.put("DATE", Date.class);
		TYPE.put("TIME", Time.class);
		TYPE.put("TIMESTAMP", Timestamp.class);
		TYPE.put("SERIAL", Integer.class);
		TYPE.put("INT4", Integer.class);
		// ...
	}

	public Row() {
		row = new ArrayList<Entry<Object, Class>>();
	}

	public <T> void add(T data) {
		row.add(new AbstractMap.SimpleImmutableEntry<Object, Class>(data, data
				.getClass()));
	}

	public void add(Object data, String sqlType) {
		Class castType = Row.TYPE.get(sqlType.toUpperCase());
		try {
			this.add(castType.cast(data));
		} catch (NullPointerException e) {
			e.printStackTrace();
			Logger lgr = Logger.getLogger(Row.class.getName());
			lgr.log(Level.SEVERE, e.getMessage() + " Add the type " + sqlType
					+ " to the TYPE hash map in the Row class.", e);
			throw e;
		}
	}

	public static void formTable(final ResultSet rs, List<Row> table)
			throws SQLException {
		if (rs == null)
			return;

		ResultSetMetaData rsmd;
		try {
			rsmd = rs.getMetaData();
			int NumOfCol = rsmd.getColumnCount();

			while (rs.next()) {
				Row current_row = new Row();

				for (int i = 1; i <= NumOfCol; i++) {
					current_row.add(rs.getObject(i), rsmd.getColumnTypeName(i));
				}

				table.add(current_row);
			}
		} catch (SQLException e) {
			throw e;
		}
	}

	public static List<String> getColumnNames(final ResultSetMetaData rsmd) throws SQLException {
		List<String> columnNames = new ArrayList<String>();
		for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
			columnNames.add(rsmd.getColumnLabel(i));
		}
		return columnNames;
	}
	
	public static List<String> getColumnTypes(final ResultSetMetaData rsmd) throws SQLException {
		List<String> columnTypes= new ArrayList<String>();
		for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
			columnTypes.add(rsmd.getColumnTypeName(i));
		}
		return columnTypes;
	}
}

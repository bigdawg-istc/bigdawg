package istc.bigdawg.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Created by ankush on 4/23/16.
 */
public class JdbcUtils {
	
	private static Logger logger = Logger.getLogger(JdbcUtils.class);
	
	/**
	 * For SciDB, use getRowsSciDB instead
	 * This does not work with SciDB because for SciDB's JDBC reference to "getObject" invariably returns null. 
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
    public static List<List<String>> getRows(final ResultSet rs) throws SQLException {
        if (rs == null) {
            return null;
        }
        List<List<String>> rows = new ArrayList<>();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int NumOfCol = rsmd.getColumnCount();
            while (rs.next()) {
                List<String> current_row = new ArrayList<String>();
                for (int i = 1; i <= NumOfCol; i++) {
                    Object value = rs.getObject(i);
                    if (value == null) {
                        current_row.add("null");
                    } else {
                        current_row.add(value.toString());
                    }
                }
                rows.add(current_row);
            }
            return rows;
        } catch (SQLException e) {
            throw e;
        }
    }

    public static List<List<String>> getRowsSciDB(final ResultSet rs) throws SQLException {
        if (rs == null) {
            return null;
        }
        List<List<String>> rows = new ArrayList<>();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int NumOfCol = rsmd.getColumnCount();
            while (!rs.isAfterLast()) {
                List<String> current_row = new ArrayList<String>();
                for (int i = 1; i <= NumOfCol; i++) {
                	Object value = null;
                	switch (rsmd.getColumnTypeName(i).toLowerCase()) {
                		case "int64":
                			value = rs.getLong(i);
                			break;
                		case "int32":
                			value = rs.getInt(i);
                			break;
                		case "string":
                			value = rs.getString(i);
                			break;
                		case "float":
                			value = rs.getFloat(i);
                			break;
                		case "double":
                			value = rs.getDouble(i);
                			break;
                		case "datetime":
                			value = rs.getDate(i);
                			break;
                		case "bool":
                			value = rs.getBoolean(i);
                			break;
                		default:
                			throw new SQLException("SciDB JDBC result set row retrieval does not support type: "+rsmd.getColumnTypeName(i).toLowerCase());
                	}
                    if (value == null) {
                        current_row.add("null");
                    } else {
                        current_row.add(value.toString());
                    }
                }
                rows.add(current_row);
                
                rs.next();
            }
            return rows;
        } catch (SQLException e) {
        	logger.debug(String.format("Last rows before exception: \n%s\n", rows));
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

    public static List<Integer> getColumnTypes(final ResultSetMetaData rsmd) throws SQLException {
        List<Integer> columnTypes = new ArrayList<>();
        for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
            columnTypes.add(rsmd.getColumnType(i));
        }
        return columnTypes;
    }

    public static List<String> getColumnTypeNames(final ResultSetMetaData rsmd) throws SQLException {
        List<String> columnTypes = new ArrayList<String>();
        for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
            columnTypes.add(rsmd.getColumnTypeName(i));
        }
        return columnTypes;
    }

    public static String printResultSet(List<List<String>> rows, List<String> colNames) {
        StringBuffer out = new StringBuffer();
        char delim = '\t';
        
//        out.append("Row");
        for (String name : colNames) {
            out.append(name).append(delim);
        }
        if (out.length() > 0) out.delete(out.length() - 1, out.length());
        
        out.append('\n');

//        int rowCounter = 1;
        for (List<String> row : rows) {
//            out.append(rowCounter + ".");
            for (String s : row) {
                out.append(s).append(delim);
            }
            if (out.length() > 0) out.delete(out.length() - 1, out.length());
            out.append('\n');
//            rowCounter += 1;
        }

        return out.toString();
    }

    public static String printResultSet(ResultSet rs) throws SQLException {
        return printResultSet(getRows(rs), getColumnNames(rs.getMetaData()));
    }
}

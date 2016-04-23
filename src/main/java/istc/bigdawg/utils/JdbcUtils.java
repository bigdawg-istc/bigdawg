package istc.bigdawg.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ankush on 4/23/16.
 */
public class JdbcUtils {
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

    public static List<String> getColumnNames(final ResultSetMetaData rsmd) throws SQLException {
        List<String> columnNames = new ArrayList<String>();
        for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
            columnNames.add(rsmd.getColumnLabel(i));
        }
        return columnNames;
    }

    public static List<String> getColumnTypes(final ResultSetMetaData rsmd) throws SQLException {
        List<String> columnTypes = new ArrayList<String>();
        for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
            columnTypes.add(rsmd.getColumnTypeName(i));
        }
        return columnTypes;
    }

    public static String printResultSet(List<List<String>> rows, List<String> colNames) {
        StringBuffer out = new StringBuffer();

        for (String name : colNames) {
            out.append("\t" + name);
        }
        out.append("\n");

        int rowCounter = 1;
        for (List<String> row : rows) {
            out.append(rowCounter + ".");
            for (String s : row) {
                out.append("\t" + s);
            }
            out.append("\n");
            rowCounter += 1;
        }

        return out.toString();
    }

    public static String printResultSet(ResultSet rs) throws SQLException {
        return printResultSet(getRows(rs), getColumnNames(rs.getMetaData()));
    }
}

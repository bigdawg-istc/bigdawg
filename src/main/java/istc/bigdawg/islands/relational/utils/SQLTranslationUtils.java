package istc.bigdawg.islands.relational.utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import javax.annotation.Nullable;
import net.sf.jsqlparser.statement.Statement;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to translate between MySQL and Postgres (and other engines)
 * Created by kateyu on 2/10/17.
 */
public class SQLTranslationUtils {
    // bidirectional map mapping MySQL datatypes to their PostgreSQL equivalents for straightforward replacements
    private static Map<String, String> mysqlToPostgres;
    private static Map<String, String> postgresToMySQL;
    private static int MAP_SIZE = 16;

    // Used https://github.com/ChrisLundquist/pg2mysql for reference
    static {
        mysqlToPostgres = new HashMap<>();
        mysqlToPostgres.put("datetime", "timestamp");
        mysqlToPostgres.put("int(11)", "integer");
        mysqlToPostgres.put("bool", "boolean");
        mysqlToPostgres.put("BLOB", "bytea");
        mysqlToPostgres.put("int(11) UNSIGNED", "int_unsigned");
        mysqlToPostgres.put("smallint UNSIGNED", "smallint_unsigned");
        mysqlToPostgres.put("bigint UNSIGNED", "bigint_unsigned");
        mysqlToPostgres.put("int(11) auto_increment", "serial");
        mysqlToPostgres.put("double", "double precision");
        mysqlToPostgres.put("float", "real");
        mysqlToPostgres.put("bool DEFAULT 1", "bool DEFAULT true");
        mysqlToPostgres.put("bool DEFAULT 0", "bool DEFAULT false");
        mysqlToPostgres.put("varchar", "character varying");

        postgresToMySQL = new HashMap<>();
        for (String k : mysqlToPostgres.keySet()) {
            postgresToMySQL.put(mysqlToPostgres.get(k), k);
        }
        postgresToMySQL.put(" without time zone", "");
        postgresToMySQL.put("now()", "CURRENT_TIMESTAMP");
    }
    /**
     * Replace MySQL types with Postgres-compatible types.
     */
    public static String convertToPostgres(String statementStr) {
        try {
            String pgStatementStr = statementStr.replace("`", "");
            pgStatementStr = pgStatementStr.replace("DEFAULT NULL", "");
            for (String key: mysqlToPostgres.keySet()) {
                pgStatementStr = pgStatementStr.replace(key, mysqlToPostgres.get(key));
            }
            Statement statement = CCJSqlParserUtil.parse(pgStatementStr);
            if (statement instanceof CreateTable) {
                CreateTable ct = (CreateTable) statement;
                // clear table options list
                ct.setTableOptionsStrings(null);
            }
            return statement.toString();
        } catch (JSQLParserException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Replace Postgres types to MySQL-compatible types.
     */
    public static String convertToMySQL(String statementStr) {
            String msStatementStr = statementStr;
            for (String key: postgresToMySQL.keySet()) {
                msStatementStr = msStatementStr.replace(key, postgresToMySQL.get(key));
            }
            return msStatementStr;
    }

    public static void main(String[] args) {
//        String statement = "CREATE TABLE d_patients (\n" +
//                "  subject_id int(11) NOT NULL,\n" +
//                "  sex varchar(1) DEFAULT NULL,\n" +
//                "  dob datetime NOT NULL,\n" +
//                "  dod datetime DEFAULT NULL,\n" +
//                "  hospital_expire_flg varchar(1) DEFAULT 'N'\n" +
//                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";
//        System.out.println(convertToPostgres(statement));
          String statement = "CREATE TABLE IF NOT EXISTS " +
                  "patients2 (subject_id integer, sex character" +
                  " varying(1), dob timestamp without time zone," +
                  " dod timestamp without time zone, hospital_expire_flg" +
                  " character varying(1))";
          System.out.println(convertToMySQL(statement));
    }
}

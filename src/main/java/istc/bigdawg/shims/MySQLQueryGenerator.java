package istc.bigdawg.shims;

import istc.bigdawg.islands.relational.operators.SQLIslandOperator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.*;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by kateyu on 2/8/17.
 */
public class MySQLQueryGenerator extends PostgreSQLQueryGenerator {
    /** log */
    private static Logger log = Logger.getLogger(MySQLQueryGenerator.class);

    @Override
    public String generateSelectIntoStatementForExecutionTree(String into) throws Exception {
        if (root == null) throw new Exception("SQLQueryGenerator, selectinto: root is null");
        super.postprocessSQLStatement((SQLIslandOperator) root);
        removeSelectDupes(dstStatement);
        String output = addCreateTableToken(dstStatement, into);
        return output;
    }

    /**
     * Add CREATE TABLE token to dstStatement. Equivalent to SELECT INTO in PostgreSQL.
     * @param dstStatement
     * @param tableName
     * @return
     */
    private String addCreateTableToken(Select dstStatement, String tableName) {
        if (tableName != null) {
              String sql = "CREATE TABLE " + tableName + " AS ";
              return sql + dstStatement.toString();
        }
        return dstStatement.toString();
    }

    /**
     * Remove duplicate fields in the select body when the statement is a join.
     * @param dstStatement
     */
    private static void removeSelectDupes(Select dstStatement) {
        PlainSelect ps = (PlainSelect) dstStatement.getSelectBody();
        List<SelectItem> sil = ps.getSelectItems();
        Expression where = ps.getWhere();
        log.debug("Removing dupes in the select body. dstStatement: "
                + dstStatement.toString() + "\nps: " + ps.toString() +
                    "\nwhere: " + where);
        if (where != null) {
            Set<String> duplicateFields = getDupesToRemove(where);
            Set<SelectItem> removed = new HashSet<>();
            for (SelectItem si : sil) {
                if (duplicateFields.contains(si.toString())) {
                    removed.add(si);
                }
            }
            for (SelectItem removedField : removed) {
                sil.remove(removedField);
            }
            ps.setSelectItems(sil);
        }
    }

    /**
     * Finds and returns a list of the fields on right hand side of any join conditions
     * to be removed in the original statement.
     * @param where expression containing possible join conditions
     * @return
     */
    private static Set<String> getDupesToRemove(Expression where) {
        Set<String> fields = new HashSet<>();
        if (where instanceof AndExpression) {
            AndExpression and = (AndExpression) where;
            fields.addAll(getDupesToRemove(and.getLeftExpression()));
            fields.addAll(getDupesToRemove(and.getRightExpression()));
        } else if (where instanceof OrExpression) {
            OrExpression or = (OrExpression) where;
            fields.addAll(getDupesToRemove(or.getLeftExpression()));
            fields.addAll(getDupesToRemove(or.getRightExpression()));
        }
        String whereStr = where.toString();
        if (whereStr.contains("=")) {
            String[] joinFields = whereStr.split("=");
            String left = joinFields[0].trim();
            String right = joinFields[1].trim();
            int leftIndex = left.lastIndexOf(".");
            int rightIndex = right.lastIndexOf(".");
            if (leftIndex != -1 && rightIndex != -1
                    && left.substring(leftIndex).equals(right.substring(rightIndex))) {
                fields.add(right);
            }
        }
        return fields;
    }


    public static void main(String[] args) {
        String sql = "SELECT" +
                " BIGDAWGSQLPRUNED_1.subject_id, BIGDAWGSQLPRUNED_1.icustay_id," +
                " BIGDAWGSQLPRUNED_1.itemid, BIGDAWGSQLPRUNED_1.ioitemid," +
                " BIGDAWGSQLPRUNED_1.charttime, BIGDAWGSQLPRUNED_1.elemid," +
                " BIGDAWGSQLPRUNED_1.cgid, BIGDAWGSQLPRUNED_1.cuid," +
                " BIGDAWGSQLPRUNED_1.amount, BIGDAWGSQLPRUNED_1.doseunits," +
                " BIGDAWGSQLPRUNED_1.route, BIGDAWGSQLPRUNED_2.hadm_id," +
                " BIGDAWGSQLPRUNED_2.subject_id, BIGDAWGSQLPRUNED_2.admit_dt," +
                " BIGDAWGSQLPRUNED_2.disch_dt FROM BIGDAWGSQLPRUNED_2," +
                " BIGDAWGSQLPRUNED_1 WHERE BIGDAWGSQLPRUNED_2.subject_id = BIGDAWGSQLPRUNED_1.subject_id" +
                " AND BIGDAWGSQLPRUNED_2.doseunits > 5";
        Select s = null;
        try {
            s = (Select) CCJSqlParserUtil.parse(sql);
            removeSelectDupes(s);
            System.out.println(s.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }
}

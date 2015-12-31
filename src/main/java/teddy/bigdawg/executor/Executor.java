package teddy.bigdawg.executor;

import istc.bigdawg.accumulo.AccumuloHandler;
import istc.bigdawg.myria.MyriaHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.scidb.SciDBHandler;
import teddy.bigdawg.planner.Planner;
import teddy.bigdawg.signature.Signature;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

public class Executor {

    static org.apache.log4j.Logger log = org.apache.log4j.Logger
            .getLogger(Executor.class.getName());

    private static Map<String, DBHandler> registeredDbHandlers;

    static {
        registeredDbHandlers = new HashMap<>();
        registeredDbHandlers.put("relational", new PostgreSQLHandler());
    }

    /**
     * Execute a given query plan, and return the result
     *
     * @param plan - a data structure of the queries to be run and their ordering, with edges pointing to dependencies
     */
    public Object executePlan(Object plan) {
        // TODO
        throw new UnsupportedOperationException();
    }

    public static QueryResult executeDSA(int querySerial, int subqueryPos, String dsa) throws SQLException {
        //String island = dsa.getIsland().toLowerCase();
        //assert(island.equals("relational")); // TODO: support other Islands

        System.out.printf("[BigDAWG] EXECUTOR: executing sub-query %d of query %d...\n", subqueryPos, querySerial);
        System.out.printf("CURRENT DATABASE :: %s\n\n", ((PostgreSQLHandler) registeredDbHandlers.get("relational")).executeQueryPostgreSQL("select current_database();").getRows().toArray());
        return ((PostgreSQLHandler) registeredDbHandlers.get("relational")).executeQueryPostgreSQL(dsa+";");
    }

    public Object migrateData(Object source, Object dest, Object data) {
        // TODO
        throw new UnsupportedOperationException();
    }
}

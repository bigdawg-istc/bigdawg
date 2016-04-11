package istc.bigdawg.monitoring;

import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.packages.CrossIslandQueryNode;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.signature.Signature;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static istc.bigdawg.postgresql.PostgreSQLHandler.getColumnNames;
import static istc.bigdawg.postgresql.PostgreSQLHandler.getRows;

/**
 * Created by chenp on 11/17/2015.
 */
public class MonitoringTask implements Runnable {
    public static final int CHECK_RATE_MS = 100;
    private final int cores;
    private final ScheduledExecutorService executor;

    /**
     * Runs in background on each machine. In lean mode, no benchmarks are run except through this.
     *
     * This is currently made with the assumption that each island resides on one machine. To adapt this, would need to add a
     * machine field to the db and choose queries based on that field..
     */
    public MonitoringTask () {
        this.executor = Executors.newScheduledThreadPool(1);
        int cores = 1;
        try {
            Process p = Runtime.getRuntime().exec("nproc");
            p.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line = "";
            while ((line = reader.readLine())!= null) {
                sb.append(line).append("\n");
            }
            String result = sb.toString().replaceAll("[^0-9]", "");
            cores = Integer.parseInt(result);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        this.cores = cores;
    }

    /**
     * Checks whether we are below MAX_CPU. If so, runs a query.
     */
    @Override
    public void run() {
        this.executor.scheduleAtFixedRate(new Task(this.cores), 0, CHECK_RATE_MS, TimeUnit.MILLISECONDS);
    }
}

class Task implements Runnable {
    private static final String RETRIEVE = "SELECT signature FROM monitoring WHERE lastRan < %d AND lastRan=(SELECT min(lastRan) FROM monitoring) ORDER BY RANDOM() LIMIT 1";
    private static final double MAX_LOAD = 0.7;
    private final int cores;

    Task(int cores){
        this.cores = cores;
    }

    @Override
    public void run(){
        if (this.can_add()) {
            try {
                final Signature signature= this.getSignature();
                if (signature != null) {
                    LinkedHashMap<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(signature.getQuery());
                    CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);
                    CrossIslandQueryNode ciqn = ciqp.getRoot();
                    List<QueryExecutionPlan> qeps = ciqn.getAllQEPs(true);

                    Monitor.runBenchmarks(qeps, signature);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Checks whether the load average is below some set threshold
     * @return true if it is currently under the threshold. false otherwise.
     */
    private boolean can_add() {
        try {
            Process p = Runtime.getRuntime().exec("uptime");
            p.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line = "";
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            Pattern currentLoad = Pattern.compile("(?<=load average: )([.0-9]+)");
            Matcher m = currentLoad.matcher(sb);
            String result = "";
            if (m.find()) {
                result = m.group();
            }
            double load = Double.parseDouble(result);
            if (load/this.cores > MAX_LOAD) {
                return false;
            }
        } catch (IOException|InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Finds the Least Recently Updated Signature for the island
     * @return Signature
     * @throws SQLException
     */
    private Signature getSignature() {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            con = PostgreSQLInstance.getConnection();
            st = con.createStatement();
            rs = st.executeQuery(String.format(RETRIEVE, System.currentTimeMillis() - MonitoringTask.CHECK_RATE_MS * 100));
            ResultSetMetaData rsmd = rs.getMetaData();
            List<String> colNames = getColumnNames(rsmd);
            List<List<String>> rows = getRows(rs);

            int signatureCol = colNames.indexOf("signature");
            if (rows.size() > 0){
                List<String> query = rows.get(0);
                if (query.size() > signatureCol){
                    return new Signature(query.get(signatureCol).replace(Monitor.stringSeparator, "'"));
                }
            }
            return null;
        } catch (Exception ex) {
            Logger lgr = Logger.getLogger(QueryClient.class.getName());
            ex.printStackTrace();
            lgr.log(Level.ERROR, ex.getMessage() + "; query: " + RETRIEVE, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(QueryClient.class.getName());
                ex.printStackTrace();
                lgr.log(Level.INFO, ex.getMessage() + "; query: " + RETRIEVE, ex);
            }
        }
        return null;
    }
}

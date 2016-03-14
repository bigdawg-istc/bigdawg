package istc.bigdawg.benchmark;

import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.planner.Planner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ankush on 3/13/16.
 */
public class SignatureTest {
    private static Logger log = Logger.getLogger(SignatureTest.class);

    // TODO(jack) or someone with knowledge of the planner/signatures
    private static final Collection<String> trainingQueries = Arrays.asList();

    // TODO(jack) or someone with knowledge of the planner/signatures
    private static final Collection<String> testQueries = Arrays.asList();

    private final Map<String, Pair<Long, Long>> results = new HashMap<>();

    @Before
    public void setup(){
    	trainingQueries.add("bdrel(SELECT count(*) FROM mimic2v26.d_patients);");
    	trainingQueries.add("bdrel(SELECT sex, count(subject_id) FROM mimic2v26.d_patients);");
    	
    	testQueries.add("bdrel(SELECT count(*) FROM mimic2v26.icd9);");
    	testQueries.add("bdrel(SELECT subject_id, count(hadm_id) FROM mimic2v26.icd9);");    	
    }
    
    
    @Test
    public void testSignature() {
        runBenchmark();
        log.info(String.format("Completed benchmark with results (left is production, right is optimal): %s", results.toString()));
    }

    public void runBenchmark() {
        // run the training queries in training mode to populate the monitor
        runQueries(trainingQueries, true);

        // run the test queries in production mode to get the signature-suggested plan
        runQueries(testQueries, false);

        // run the test queries in training mode to get the first-returned plan
        runQueries(testQueries, true);
    }

    public void runQueries(Collection<String> tests, boolean isTrainingMode) {

        for(String test : testQueries) {
            long runtime;
            long start = System.currentTimeMillis();
            try {
                Planner.processQuery(test, isTrainingMode);
                runtime = System.currentTimeMillis() - start;
            } catch (Exception e) {
                e.printStackTrace();
                runtime = Long.MAX_VALUE;
            }

            Pair<Long, Long> result = results.getOrDefault(test, new ImmutablePair<>(Long.MAX_VALUE, Long.MAX_VALUE));
            if(isTrainingMode) {
                // runtime is fastest result of all possible plans
                results.put(test, new ImmutablePair<>(result.getLeft(), runtime));
            } else {
                // runtime used signature model to predict plan
                results.put(test, new ImmutablePair<>(runtime, result.getRight()));
            }
        }

        while(!Monitor.allQueriesDone()){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

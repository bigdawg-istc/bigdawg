package istc.bigdawg.benchmark;

import istc.bigdawg.planner.Planner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ankush on 3/13/16.
 */
public class SignatureTest {

    public static Collection<String> trainingQueries = Arrays.asList(); // TODO(jack?)
    public static Collection<String> testQueries = Arrays.asList(); // TODO(jack?)
    
    @Test
    public void testSignature() {
        runBenchmark();
        processResults();
    }

    public void runBenchmark() {
        setTrainingMode(true);
        runQueries(trainingQueries);

        setTrainingMode(false);
        runQueries(testQueries);

        setTrainingMode(true);
        runQueries(testQueries);
    }

    public void setTrainingMode(boolean trainingMode) {
        // TODO(jack): set system to proper mode of execution somehow
        // either keep track of it in this test and allow it to be passed into the Planner
        // or allow the Planner to keep track of it?
    }

    public void runQueries(Collection<String> tests) {
        for(String test : testQueries) {
            // this blocks until each one completes if in production mode
            try {
                Planner.processQuery(test);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // TODO(peinan): somehow block until everything completes if in training mode
    }

    public Map<String, Pair<Long, Long>> processResults() {
        Map<String, Pair<Long, Long>> results = new HashMap<>();

        for(String query : testQueries) {
            // TODO: get production time from monitor output file
            long production = 0;

            // TODO: get minimum time for this query from monitor output file
            long bestFromTraining = 0;

            results.put(query, ImmutablePair.of(production, bestFromTraining));
        }

        return results;
    }

}

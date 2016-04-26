package istc.bigdawg.executor.shuffle;

import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;

import java.util.Collection;

/**
 * Created by ankush on 4/25/16.
 */
public interface ShuffleEngine {

    public enum HistogramStrategy {
        EXHAUSTIVE,
        SAMPLING
    }

    public default Collection<Histogram> createHistograms(Collection<BinaryJoinExecutionNode.JoinOperand> operands, HistogramStrategy strategy) {
        switch(strategy) {
            case EXHAUSTIVE:
                return createHistogramsExhaustively(operands);
            case SAMPLING:
                return createHistogramsBySampling(operands);
            default:
                return null;
        }
    }

    public Collection<Histogram> createHistogramsExhaustively(Collection<BinaryJoinExecutionNode.JoinOperand> operands);
    public Collection<Histogram> createHistogramsBySampling(Collection<BinaryJoinExecutionNode.JoinOperand> operands);
}

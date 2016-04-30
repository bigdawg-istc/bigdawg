package istc.bigdawg.executor;

import java.util.Optional;

/**
 * Created by ankush on 4/21/16.
 */
public interface ExecutorEngine {
    Optional<QueryResult> execute(String query) throws LocalQueryExecutionException;

    class LocalQueryExecutionException extends Exception {
        public LocalQueryExecutionException() {
            super();
        }
        public LocalQueryExecutionException(String message) {
            super(message);
        }
        public LocalQueryExecutionException(Throwable cause) {
            super(cause);
        }
        public LocalQueryExecutionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

package istc.bigdawg.exceptions;

public class ApiException extends Exception {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public ApiException(String message) {
        super(message);
    }

    public ApiException(String message, Throwable cause) {
        super(message, cause);
    }
}

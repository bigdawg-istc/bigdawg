/**
 * 
 */
package istc.bigdawg.network;

import org.apache.log4j.Logger;

/**
 * @author Adam Dziedzic
 * 
 *         This is a debug message to be sent via network.
 */
public class DebugMessage implements NetworkObject {

	/* log */
	private static Logger log = Logger.getLogger(DebugMessage.class);

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 1L;

	/** message to be sent via network */
	private final String message;

	/**
	 * 
	 */
	public DebugMessage(final String message) {
		this.message = message;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.network.NetworkObject#execute()
	 */
	@Override
	public Object execute() throws Exception {
		log.debug("Execution of debug message.");
		return message + " ---> was processed by server;";
	}

}

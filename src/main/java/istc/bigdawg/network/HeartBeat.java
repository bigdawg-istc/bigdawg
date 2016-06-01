/**
 * 
 */
package istc.bigdawg.network;

import org.apache.log4j.Logger;

/**
 * This object checks if the current connection is active.
 * 
 * @author Adam Dziedzic
 * 
 *         Mar 9, 2016 5:23:05 PM
 */
public class HeartBeat implements NetworkObject {

	/* log */
	private static Logger log = Logger.getLogger(HeartBeat.class);

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 1L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.network.NetworkObject#execute()
	 */
	@Override
	public Object execute() throws Exception {
		log.debug("Execution of heart beat message.");
		return true;
	}

}

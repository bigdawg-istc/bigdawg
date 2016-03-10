/**
 * 
 */
package istc.bigdawg.network;

/**
 * This object checks if the current connection is active.
 * 
 * @author Adam Dziedzic
 * 
 * Mar 9, 2016 5:23:05 PM
 */
public class HeartBeat implements NetworkObject {

	/**
	 * The objects of the class are serializable. 
	 */
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see istc.bigdawg.network.NetworkObject#execute()
	 */
	@Override
	public Object execute() throws Exception {
		return true;
	}

}

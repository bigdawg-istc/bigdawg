/**
 * 
 */
package istc.bigdawg.zookeeper;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class ZnodePathMessage {

	/* Full path to the znode in ZooKeeper. */
	private String znodePath;

	/**
	 * Information about the znode (e.g. how it was created). It can be seen as
	 * a debug information.
	 */
	private String message;

	public ZnodePathMessage() {
	}

	public ZnodePathMessage(String znodePath, String message) {
		this.znodePath = znodePath;
		this.message = message;
	}

	/**
	 * @return the znodePath
	 */
	public String getZnodePath() {
		return znodePath;
	}

	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * @param znodePath
	 *            the znodePath to set
	 */
	public void setZnodePath(String znodePath) {
		this.znodePath = znodePath;
	}

	/**
	 * @param message
	 *            the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}
}

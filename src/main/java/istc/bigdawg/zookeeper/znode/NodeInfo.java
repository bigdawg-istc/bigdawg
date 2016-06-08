/**
 * 
 */
package istc.bigdawg.zookeeper.znode;

import java.time.Instant;

/**
 * @author Adam Dziedzic
 * 
 *         To store information in znodes in ZooKeeper (this is for production
 *         mode, in the dev mode, please use bytes from the String object.
 */
public class NodeInfo implements ZooKeeperData {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/* General information about the znode. */
	private String information;

	/*
	 * The number of milliseconds since January 1, 1970, 00:00:00 GMT
	 * 
	 * The creation time is useful only in the creator (there can be time skew
	 * between separate physical machines).
	 * 
	 */
	private Instant creationTime;

	/**
	 * 
	 */
	public NodeInfo() {
		this.creationTime = Instant.now();
	}

	/**
	* 
	*/
	public NodeInfo(String information) {
		this();
		this.information = information;
	}

	/**
	 * @return the creationTime of this info
	 */
	public Instant getCreationTime() {
		return creationTime;
	}

	/**
	 * @return the information
	 */
	public String getInformation() {
		return information;
	}

}

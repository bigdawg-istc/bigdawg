/**
 * 
 */
package istc.bigdawg.zookeeper;

import java.time.Instant;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class NodeInfo implements ZooKeeperData {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
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
	 * @return the creationTime
	 */
	public Instant getCreationTime() {
		return creationTime;
	}

}

/**
 * 
 */
package istc.bigdawg.network;

import java.io.Serializable;

/**
 * This is an interface for a class whose objects should be serializable and we
 * should be able to send them via network.
 * 
 * @author Adam Dziedzic
 * 
 */
public interface NetworkObject extends Serializable {

	/** execute the commands in the object and return its result */
	abstract public Object execute() throws Exception;
	
}

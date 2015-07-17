/**
 * 
 */
package istc.bigdawg.accumulo;

import java.util.List;

/**
 * @author Adam Dziedzic
 *
 */
public interface Parameters {
	String getInstanceName();
	List<String> getZooServers();
	String getUserName();
	String getPassword();
}

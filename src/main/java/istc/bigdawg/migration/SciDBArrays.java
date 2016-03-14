/**
 * 
 */
package istc.bigdawg.migration;

/**
 * Names of flat and destination arrays used during the migration process.
 * 
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 11:03:35 AM
 */
public class SciDBArrays {

	private String flat;
	private String multiDimensional;

	/**
	 * @param flat
	 * @param multiDimensional
	 */
	public SciDBArrays(String flat, String multiDimensional) {
		this.flat = flat;
		this.multiDimensional = multiDimensional;
	}

	/**
	 * @return the flat
	 */
	public String getFlat() {
		return flat;
	}

	/**
	 * @return the multiDimensional
	 */
	public String getMultiDimensional() {
		return multiDimensional;
	}

}

/**
 * 
 */
package istc.bigdawg.migration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Flat and destination arrays used during the migration process.
 * 
 * @author Adam Dziedzic
 * 
 */
public class SciDBArrays implements Serializable {

	/**
	 * Serial version of the class (for serialization).
	 */
	private static final long serialVersionUID = -6536294395139331080L;

	/**
	 * A flat array in SciDB, it contains only an auxiliary dimension (e.g. i -
	 * which is an index of a record)
	 */
	private SciDBArray flat;

	/**
	 * A multidimensional array contains specific dimensions, for example, an
	 * multi-dim array representing a canvas contains two dimensions: x and y.
	 */
	private SciDBArray multiDimensional;

	public SciDBArrays(SciDBArray flat, SciDBArray multiDimensional) {
		this.flat = flat;
		this.multiDimensional = multiDimensional;
	}

	/**
	 * @return the flat
	 */
	public SciDBArray getFlat() {
		return flat;
	}

	/**
	 * @return the multiDimensional
	 */
	public SciDBArray getMultiDimensional() {
		return multiDimensional;
	}

	/**
	 * 
	 * @return Names of all the arrays which were created during the migration.
	 */
	public Set<String> getCreatedArrays() {
		Set<String> createdArrays = new HashSet<>();
		for (SciDBArray array : Arrays.asList(flat, multiDimensional)) {
			if (array != null && array.wasCreated()) {
				createdArrays.add(array.getName());
			}
		}
		return createdArrays;
	}

	/**
	 * 
	 * @return Names of all intermediate (flat) arrays.
	 */
	public Set<String> getIntermediateArrays() {
		Set<String> intermediateArrays = new HashSet<>();
		for (SciDBArray array : Arrays.asList(flat, multiDimensional)) {
			if (array != null && array.isItermediate()) {
				intermediateArrays.add(array.getName());
			}
		}
		return intermediateArrays;
	}

}

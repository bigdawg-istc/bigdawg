/**
 * 
 */
package istc.bigdawg.migration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Flat and destination arrays used during the migration process.
 * 
 * @author Adam Dziedzic
 * 
 */
public class SciDBArrays {

	private SciDBArray flat;
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

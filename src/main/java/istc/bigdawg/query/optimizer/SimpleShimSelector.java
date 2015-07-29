package istc.bigdawg.query.optimizer;

import istc.bigdawg.Island;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.exceptions.NotSupportIslandException;

public class SimpleShimSelector implements ShimSelector {


	public Shim selectShim(String target, Island island) throws NotSupportIslandException{
		if (island == Island.ARRAY) {
			return Shim.PSQLARRAY;
		} else if (island == Island.RELATION) {
			return Shim.PSQLRELATION;
		} else if (island == Island.D4M) {
			throw new NotSupportIslandException("Unsupported Island impl " + island.toString());
		} else if (island == Island.MYRIA) {
			return Shim.MYRIA;
		} else if (island == Island.TEXT) {
			return Shim.ACCUMULOTEXT;
		} else {
			throw new NotSupportIslandException("Unknown Island impl " + island.toString() + " for target: " + target);
		}
	}

}

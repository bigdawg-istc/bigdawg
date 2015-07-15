package istc.bigdawg.query.optimizer;

import istc.bigdawg.Island;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.exceptions.NotSupportIslandException;

public interface ShimSelector {
	public Shim selectShim(String target, Island island) throws NotSupportIslandException;
}
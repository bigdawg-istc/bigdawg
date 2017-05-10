package istc.bigdawg.exceptions;

import istc.bigdawg.islands.IslandAndCastResolver.Scope;

public class UnsupportedIslandException extends IslandException {

	public UnsupportedIslandException(String token) {
		super(String.format("Unknown island token: %s", token));
	}
	
	public UnsupportedIslandException(Scope scope, String source) {
		super(String.format("Unsupported Island: %s; Evoking function: %s", scope.name(), source));
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6015587421902438478L;

}

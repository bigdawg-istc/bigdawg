package istc.bigdawg.islands.text;

import java.util.Map;

import istc.bigdawg.islands.AbstractNonRelationalIslandQuery;
import istc.bigdawg.islands.IslandAndCastResolver.Scope;

class TextIslandQuery extends AbstractNonRelationalIslandQuery {
	TextIslandQuery(String islandQuery, String name, Map<String, String> transitionSchemas)
			throws Exception {
		super(Scope.TEXT, islandQuery, name, transitionSchemas);
	}
}

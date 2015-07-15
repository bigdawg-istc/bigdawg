package istc.bigdawg.query.parser;

import istc.bigdawg.query.ASTNode;

public interface Parser {
	public ASTNode parseQueryIntoTree(String text);
}

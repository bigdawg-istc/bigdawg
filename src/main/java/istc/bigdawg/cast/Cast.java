package istc.bigdawg.cast;

public class Cast {
	
	private String objectName;
	private String fromIsland;
	private String toIsland;
	private String identifier;
	
	public Cast(String objName, String from, String to, String id) throws Exception {
		this.setObjectName(objName);
		this.setFromIsland(from);
		this.setToIsland(to);
		this.setIdentifier(id);
	}

	public void print() {
		System.out.println("Type      : Cast");
		System.out.println("Identifier: "+identifier);
		System.out.println("Object    : "+objectName);
		System.out.println("From      : "+fromIsland);
		System.out.println("To        : "+toIsland);
	}
	
	public String getObjectName() {
		return objectName;
	}

	private void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	public String getFromIsland() {
		return fromIsland;
	}

	private void setFromIsland(String fromIsland) {
		this.fromIsland = fromIsland;
	}

	public String getToIsland() {
		return toIsland;
	}

	private void setToIsland(String toIsland) {
		this.toIsland = toIsland;
	}

	public String getIdentifier() {
		return identifier;
	}

	private void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	
}
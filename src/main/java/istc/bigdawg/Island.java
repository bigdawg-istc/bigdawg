package istc.bigdawg;


import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum Island {
	RELATION, ARRAY, TEXT, STREAM, D4M, MYRIA;
	
    protected static final Map<Integer, Island> idx_lookup = new HashMap<Integer, Island>();
    protected static final Map<String, Island> name_lookup = new HashMap<String, Island>();
    static {
        for (Island vt : EnumSet.allOf(Island.class)) {
        	Island.idx_lookup.put(vt.ordinal(), vt);
        	Island.name_lookup.put(vt.name().toUpperCase(), vt);
        } // FOR
    }

	
    public static Island get(String name) {
        return (Island.name_lookup.get(name.trim().toUpperCase()));
    }
}

package istc.bigdawg.islands.SciDB;

import java.util.ArrayList;
import java.util.List;

public class AFLPlanAttribute {
	
	public String name;
	public int indent;
	public List<String> properties;
	public List<AFLPlanAttribute> subAttributes;
	
	public AFLPlanAttribute() {
		properties = new ArrayList<>();
		subAttributes = new ArrayList<>();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		int l;
		
//		for (int i = 0; i < indent; ++i) sb.append(' ');
		sb.append('(').append(name);
		l = properties.size();
		for (int i = 0; i < l; ++i) sb.append(' ').append(properties.get(i));
//		sb.append('\n');
		l = subAttributes.size();
		if (l > 0) sb.append(' ').append('(');
		for (int i = 0; i < l; ++i) {
			if (i > 0) sb.append(' ');
			sb.append(subAttributes.get(i));
		}
		if (l > 0) sb.append(')');
		return sb.append(')').toString();
	}
}

package istc.bigdawg.packages;

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
		
		for (int i = 0; i < indent; ++i) sb.append(' ');
		sb.append(name);
		l = properties.size();
		for (int i = 0; i < l; ++i) sb.append(' ').append(properties.get(i));
		sb.append('\n');
		l = subAttributes.size();
		for (int i = 0; i < l; ++i) sb.append(subAttributes.get(i));
		
		return sb.toString();
	}
}

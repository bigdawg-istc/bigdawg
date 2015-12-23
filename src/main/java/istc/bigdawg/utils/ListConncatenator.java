/**
 * 
 */
package istc.bigdawg.utils;

import java.util.List;

/**
 * @author Adam Dziedzic
 * 10:53:44 AM
 *
 */
public class ListConncatenator {

	public static String joinList(String[] list, char separator,String lastCharacter) {
		StringBuilder stringBuilder = new StringBuilder();
		for (int i=0;i<list.length;++i) {
			if (i>0) {
				stringBuilder.append(separator);
			}
			stringBuilder.append(list[i]);
		}
		stringBuilder.append(lastCharacter);
		return stringBuilder.toString();
	}

}

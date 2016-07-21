package istc.bigdawg.islands.SStore;

import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.exceptions.BigDawgException;

public class SStoreQueryParser {
	
	private StringBuilder inputSB;
	StringBuilder tempSB;
	
	public SStoreQueryParser() {
		inputSB = new StringBuilder();
		tempSB = new StringBuilder();
	}
	
	public List<String> parse(String input) throws BigDawgException {
		List<String> result = new ArrayList<>();
		
		inputSB.append(input);
		
		while (inputSB.length() > 0) {
			processSingleToken(result);
			inputSB.deleteCharAt(0);
		}
		result.add(tempSB.toString());
		
		System.out.printf("StringBuilder from SStoreQueryParser: %s; result: %s\n", inputSB, result);
		return result;
	}
	
	private void processSingleToken(List<String> result) {
		char token = inputSB.charAt(0);
		switch (token) {
		case ' ':
		case '\t':
			break;
		case ',':
			result.add(tempSB.toString());
			tempSB = new StringBuilder();
			break;
		default:
			tempSB.append(token);
			break;
		}
	}
	
}

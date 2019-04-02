package istc.bigdawg.islands.api;

import istc.bigdawg.exceptions.BigDawgException;

import java.util.ArrayList;
import java.util.List;

/**
 * Not used any more - from initial Proof of Concept
 */

public class ApiQueryParser {

    private StringBuilder inputSB;
    private StringBuilder tempSB;

    public ApiQueryParser() {
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

		System.out.printf("StringBuilder from ApiQueryParser: %s; result: %s\n", inputSB, result);
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

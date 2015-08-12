/**
 * 
 */
package istc.bigdawg.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author adam
 * 
 */
public enum ObjectMapperResource {
	INSTANCE;

	private ObjectMapperResource() {
		objectMapper = new ObjectMapper();
	}

	private final ObjectMapper objectMapper;

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

}

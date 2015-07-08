package istc.bigdawg;

public class RegisterStreamResponse {
	private String message;
	private int responseCode;
	private String statusURL;
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public int getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}
	public String getStatusURL() {
		return statusURL;
	}
	public void setStatusURL(String statusURL) {
		this.statusURL = statusURL;
	}
	public RegisterStreamResponse(String message, int responseCode,
			String statusURL) {
		super();
		this.message = message;
		this.responseCode = responseCode;
		this.statusURL = statusURL;
	}
	
	
	

}

package istc.bigdawg;

public class RegisterStreamRequest {
	private boolean oneTime;
	private String notifyURL;
	private String query;
	private AuthorizationRequest authorization;
	private boolean pushNotify;
	
	public boolean isOneTime() {
		return oneTime;
	}
	public void setOneTime(boolean oneTime) {
		this.oneTime = oneTime;
	}
	public String getNotifyURL() {
		return notifyURL;
	}
	public void setNotifyURL(String notifyURL) {
		this.notifyURL = notifyURL;
	}
	public boolean isPushNotify() {
		return pushNotify;
	}
	public void setPushNotify(boolean pushNotify) {
		this.pushNotify = pushNotify;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public AuthorizationRequest getAuthorization() {
		return authorization;
	}
	public void setAuthorization(AuthorizationRequest authorization) {
		this.authorization = authorization;
	}
	
	

}

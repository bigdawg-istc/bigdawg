package istc.bigdawg.query;

import istc.bigdawg.AuthorizationRequest;
import istc.bigdawg.interfaces.Request;

import java.sql.Timestamp;

public class QueryRequest implements Request{
	
	private String query;
	private AuthorizationRequest authorization;
	private int tuplesPerPage;
	private int pageNumber;
	private Timestamp timestamp;
	
	public String getQuery() {
		return query;
	}
	
	public void setQuery(final String query) {
		this.query=query;
	}
	
	public AuthorizationRequest getAuthorization(){
		return authorization;
	}
	
	public void setAuthorization(final AuthorizationRequest authorization) {
		this.authorization=authorization;
	}
	
	public int getTuplesPerPage() {
		return tuplesPerPage;
	}
	
	public void setTuplesPerPage(final int tuplesPerPage) {
		this.tuplesPerPage=tuplesPerPage;
	}
	
	public int getPageNumber() {
		return pageNumber;
	}
	
	public void setPageNumber(final int pageNumber) {
		this.pageNumber=pageNumber;
	}
	
	public Timestamp getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(final Timestamp timestamp) {
		this.timestamp=timestamp;
	}
}
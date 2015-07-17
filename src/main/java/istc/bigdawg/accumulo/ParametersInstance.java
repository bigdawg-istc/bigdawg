package istc.bigdawg.accumulo;

import java.util.List;

public class ParametersInstance implements Parameters {

	private String instanceName;
	private List<String> zooServers;
	private String userName;
	private String password;
	
	public ParametersInstance(String instanceName, List<String> zooServers, String userName, String password) {
		this.instanceName=instanceName;
		this.zooServers=zooServers;
		this.userName=userName;
		this.password=password;
	}

	@Override
	public String getInstanceName() {
		return instanceName;
	}

	@Override
	public List<String> getZooServers() {
		return zooServers;
	}

	@Override
	public String getUserName() {
		return userName;
	}

	@Override
	public String getPassword() {
		return password;
	}

	public static void main(String[] args) {
	}

}

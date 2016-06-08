/**
 * 
 */
package istc.bigdawg.network;

/**
 * @author Adam Dziedzic
 * 
 *         Store information about the pair: ip address and port.
 */
public class IpAddressPort implements Comparable<IpAddressPort> {

	/**
	 * ip address
	 */
	private String IpAddress;

	/** port number */
	private String port;

	public IpAddressPort(String ipAddress, String port) {
		super();
		IpAddress = ipAddress;
		this.port = port;
	}

	public String getIpAddress() {
		return IpAddress;
	}

	public String getPort() {
		return port;
	}

	@Override
	public String toString() {
		return "IpAddressPort [IpAddress=" + IpAddress + ", port=" + port + "]";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((IpAddress == null) ? 0 : IpAddress.hashCode());
		result = prime * result + ((port == null) ? 0 : port.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IpAddressPort other = (IpAddressPort) obj;
		if (IpAddress == null) {
			if (other.IpAddress != null)
				return false;
		} else if (!IpAddress.equals(other.IpAddress))
			return false;
		if (port == null) {
			if (other.port != null)
				return false;
		} else if (!port.equals(other.port))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(IpAddressPort other) {
		if (IpAddress == null || other.IpAddress == null || port == null
				|| other.port == null) {
			throw new IllegalArgumentException(
					"IP address and port have to be provided for comparison.");
		}
		int firstComparison = IpAddress.compareTo(other.IpAddress);
		if (firstComparison != 0) {
			return firstComparison;
		}
		return port.compareTo(other.port);
	}

}
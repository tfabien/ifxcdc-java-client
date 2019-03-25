/* 
 * @author dhudson -  
 * Created 29 Jul 2010 : 12:11:07 
 */

package com.conforama.informix.cdc;

/**
 * Used to specify the Connection Details for a CDC Connection.
 * <P>
 * 
 * @author pwalsh
 * 
 */
public final class CDCConnectionDetails {

	private final String theConnectionString;

	private String username = null;
	private String password = null;
	private String informixServer = null;
	private boolean isDebugging = false;	
	private int cdcTimeout = 10;
	private int maxRecordsPerReturn = 1;
	private int interfaceMajorVersion = 1;
	private int interfaceMinorVersion = 1;

	/**
	 * Constructor.
	 * <P>
	 * The connection String should look like the following..
	 * 
	 * <pre>
	 * 
	 * // 192.168.52.12:9088/syscdcv1
	 * </pre>
	 * 
	 * 
	 * @param connectionString
	 */
	public CDCConnectionDetails(String connectionString) {

		if (!connectionString.endsWith(":")) {
			this.theConnectionString = connectionString + ":";
		} else {
			this.theConnectionString = connectionString;
		}

	}

	/**
	 * Sets the name of the Informix server.
	 * <p>
	 * 
	 * @param informixServer
	 *            the Informix Server name.
	 */
	public void setInformixServer(String informixServer) {
		this.informixServer = informixServer;
	}

	/**
	 * Return the name of the Informix Server.
	 * <p>
	 * 
	 * @return the Informix server
	 */
	public String getInformixServer() {
		return informixServer;
	}

	/**
	 * Sets the username for the connection
	 * <p>
	 * 
	 * @param username
	 *            the user name.
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Returns the user name.
	 * <P>
	 * 
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Sets the password for the connection
	 * <p>
	 * 
	 * @param password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Indicates whether debugging is set on for the connection.
	 * <p>
	 * 
	 * @return true if the connection is to have debugging enabled
	 */
	public boolean isDebugging() {
		return isDebugging;
	}

	/**
	 * Sets debugging flag for the connection.
	 * <p>
	 * 
	 * @param value
	 *            true to debug
	 */
	public void setDebugging(boolean value) {
		this.isDebugging = value;
	}
	
	public int getCdcTimeout() {
		return cdcTimeout;
	}

	public void setCdcTimeout(int cdcTimeout) {
		this.cdcTimeout = cdcTimeout;
	}

	public int getMaxRecordsPerReturn() {
		return maxRecordsPerReturn;
	}

	public void setMaxRecordsPerReturn(int maxRecordsPerReturn) {
		this.maxRecordsPerReturn = maxRecordsPerReturn;
	}

	public int getInterfaceMajorVersion() {
		return interfaceMajorVersion;
	}

	public void setInterfaceMajorVersion(int interfaceMajorVersion) {
		this.interfaceMajorVersion = interfaceMajorVersion;
	}

	public int getInterfaceMinorVersion() {
		return interfaceMinorVersion;
	}

	public void setInterfaceMinorVersion(int interfaceMinorVersion) {
		this.interfaceMinorVersion = interfaceMinorVersion;
	}

	public String getTheConnectionString() {
		return theConnectionString;
	}

	public String getPassword() {
		return password;
	}

	/**
	 * Get the JDBC URL
	 * 
	 * @return the qualified JDBC URL String
	 */
	String getJDBCUrl() {
		StringBuilder builder = new StringBuilder("jdbc:informix-sqli:");
		builder.append(theConnectionString);
		builder.append("informixserver=");
		builder.append(informixServer);
		builder.append(";user=");
		builder.append(username);
		builder.append(";password=");
		builder.append(password);

		return builder.toString();
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getJDBCUrl() + " debug [" + isDebugging + "]";
	}
}
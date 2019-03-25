package com.conforama.informix.cdc;

import java.io.PrintWriter;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conforama.informix.cdc.model.CDCMessageFactory;
import com.conforama.informix.cdc.model.common.CDCRecord;
import com.informix.jdbc.IfxSmartBlob;

public final class CDCConnection {

	private static Logger log = LoggerFactory.getLogger(CDCConnection.class);

	private int READ_DATA_BUFFER_LENGTH = 40960;

	private CDCConnectionDetails connectionDetails = null;

	private Connection connection;

	private int sessionID;

	private int nextUserData = 0;

	private static final String INFORMIX_DRIVER_CLASS = "com.informix.jdbc.IfxDriver";

	/**
	 * Create a new CDC connection.
	 * <P>
	 * If this constructor is used then connection details must be explictly
	 * supplied using {@link #setConnectionDetails(CDCConnectionDetails)} before
	 * connecting.
	 */
	public CDCConnection() {
		this(null);
	}

	/**
	 * Create a new CDC Connection with supplied connection details.
	 * 
	 * @param connectionDetails
	 *            the connection details
	 */
	public CDCConnection(CDCConnectionDetails connectionDetails) {
		this.connectionDetails = connectionDetails;
	}

	/**
	 * Set the connection details
	 * <p>
	 * 
	 * @param connectionDetails
	 *            the connection details to set
	 */
	public void setConnectionDetails(CDCConnectionDetails connectionDetails) {
		this.connectionDetails = connectionDetails;
	}

	/**
	 * Returns the connection details
	 * <p>
	 * 
	 * @return the connection details or null if not set
	 */
	public CDCConnectionDetails getConnectionDetails() {
		return connectionDetails;
	}

	/**
	 * This opens a connection to the database and creates a CDC Session
	 * 
	 * @throws Exception
	 *             if unable to connect
	 */
	public void connect() throws Exception {

		if (connectionDetails == null) {
			throw new Exception("CDCConnection: no connection details supplied");
		}

		try {
			Class.forName(INFORMIX_DRIVER_CLASS);
		} catch (ClassNotFoundException ex) {
			throw new Exception("CDCConnection: Unable to find class " + INFORMIX_DRIVER_CLASS, ex);
		}

		try {
			if (connectionDetails.isDebugging()) {
				DriverManager.setLogWriter(new PrintWriter(System.out));
			}
			log.debug("[connect] Connect to database");
			connection = DriverManager.getConnection(connectionDetails.getJDBCUrl());
			connection.setReadOnly(true);
		} catch (SQLException ex) {
			throw new Exception("CDCConnection: unable to connect to the database : " + ex.getLocalizedMessage(), ex);
		}

		try {
			log.debug("[connect] Open CDC session - cdc_opensess");
			CallableStatement cstmt = connection.prepareCall("execute function informix.cdc_opensess(?,?,?,?,?,?)");

			cstmt.setString(1, connectionDetails.getInformixServer());

			// Assign Session ID : Must be zero
			cstmt.setInt(2, 0);

			// Timeout... < 0 wait for ever.. 0 return immediately if no data >
			// 0 number of seconds to wait
			cstmt.setInt(3, connectionDetails.getCdcTimeout());

			// Max records per return..
			cstmt.setInt(4, connectionDetails.getMaxRecordsPerReturn());

			// Interface behaviour Major version Must be 1
			cstmt.setInt(5, connectionDetails.getInterfaceMajorVersion());

			// Interface behaviour Minor version Must be 1
			cstmt.setInt(6, connectionDetails.getInterfaceMinorVersion());

			ResultSet rs = cstmt.executeQuery();

			rs.next();

			// If positive int all OK...
			sessionID = rs.getInt(1);

			if (sessionID < 0) {
				int errCode = rs.getInt(1);
				cstmt.close();
				rs.close();
				throw new Exception("CDCConnection: Unable to get CDC session: " + getErrorDescription(errCode));
			}
		} catch (SQLException ex) {
			throw new Exception("CDCConnection: unable to open cdc session " + ex.getLocalizedMessage(), ex);
		}
	}

	public void disconnect() throws Exception {
		try {
			log.debug("[connect] Open CDC session - cdc_opensess");
			CallableStatement cstmt = connection.prepareCall("execute function informix.cdc_closesess(?)");
			cstmt.setLong(1, getCDCSessionID());
			ResultSet rs = cstmt.executeQuery();
			rs.next();
			// If positive int all OK...
			sessionID = rs.getInt(1);
			if (sessionID < 0) {
				int errCode = rs.getInt(1);
				cstmt.close();
				rs.close();
				throw new Exception("CDCConnection: Unable to close CDC session: " + getErrorDescription(errCode));
			}
		} catch (SQLException ex) {
			throw new Exception("CDCConnection: Unable to close cdc CDC session " + ex.getLocalizedMessage(), ex);
		}
	}

	/**
	 * Specify table and columns to capture.
	 * <P>
	 * Specifies a table and columns within that table from which to start capturing
	 * data. You cannot include columns with simple large objects, user-defined data
	 * types, or collection data types.
	 * <p>
	 * 
	 * @param qualifiedTableName
	 *            For example database:owner.table (i.e. test:informix.Outcome)
	 * 
	 * @param cols
	 *            A comma separated list of column names.
	 * 
	 * @return an ID to reference with this table name, for operational records this
	 *         will be user data
	 * 
	 * @throws Exception
	 *             if unable to enable capture using specified table and columns
	 */
	public int enableCapture(String qualifiedTableName, String cols) throws Exception {

		int userData = 0;
		int resultCode;
		CallableStatement cstmt;
		ResultSet rs;

		try {
			log.debug("[enableCapture] Open CDC session - cdc_set_fullrowlogging");
			cstmt = connection.prepareCall("execute function informix.cdc_set_fullrowlogging(?,?)");

			// Table to capture
			cstmt.setString(1, qualifiedTableName);

			// mode=1 - Start. mode=0 - Stop
			cstmt.setInt(2, 1);

			rs = cstmt.executeQuery();
			rs.next();

			resultCode = rs.getInt(1);

			if (resultCode != 0) {
				throw new Exception(
						"CDCConnection: Unable to set full row logging: " + getErrorDescription(resultCode));
			}

		} catch (SQLException ex) {
			throw new Exception("CDCConnection: Unable to set full row logging ", ex);
		}

		try {
			log.debug("[enableCapture] Start CDC capture - cdc_startcapture");
			cstmt = connection.prepareCall("execute function informix.cdc_startcapture(?,?,?,?,?)");
			cstmt.setInt(1, sessionID);

			// Must be zero
			cstmt.setLong(2, 0);

			cstmt.setString(3, qualifiedTableName);
			cstmt.setString(4, cols);

			// Thread safe
			userData = nextUserData++;

			// My user data...
			cstmt.setInt(5, userData);

			rs = cstmt.executeQuery();
			rs.next();

			resultCode = rs.getInt(1);

			if (resultCode != 0) {
				throw new Exception("CDCConnection: Unable to start cdc capture: " + getErrorDescription(resultCode));
			}
		} catch (SQLException ex) {
			throw new Exception("CDCConnection: Unable to start cdc capture ", ex);
		}

		return userData;
	}

	/**
	 * Stop capturing data from a specific table.
	 * <P>
	 * This function does not affect the session status; the session remains open
	 * and active.
	 * 
	 * @param qualifiedTableName
	 *            should be in the form of database:owner.table
	 * @throws Exception
	 *             if unable to stop
	 */
	public void disableCapture(String qualifiedTableName) throws Exception {
		try {
			log.debug("[disableCapture] End CDC capture - cdc_endcapture");
			CallableStatement cstmt = connection.prepareCall("execute function informix.cdc_endcapture(?,?,?)");

			cstmt.setInt(1, sessionID);
			cstmt.setLong(2, 0);
			cstmt.setString(3, qualifiedTableName);

			ResultSet rs = cstmt.executeQuery();
			rs.next();

			int resultCode = rs.getInt(1);

			if (resultCode != 0) {
				throw new Exception("CDCConnection: Unable to end cdc capture " + getErrorDescription(resultCode));
			}

		} catch (SQLException ex) {
			throw new Exception("CDCConnection: Unable to end cdc capture ", ex);
		}
	}

	/**
	 * Start the capture process.
	 * <p>
	 * 
	 * @throws Exception
	 *             if unable to start capture.
	 */
	public void startCapture(long position) throws Exception {
		try {
			log.debug("[startCapture] Activate CDC session - cdc_activatesess");
			CallableStatement cstmt = connection.prepareCall("execute function informix.cdc_activatesess(?,?)");
			cstmt.setInt(1, sessionID);
			cstmt.setLong(2, position);

			ResultSet rs = cstmt.executeQuery();
			rs.next();
			int resultCode = rs.getInt(1);

			if (resultCode != 0) {
				throw new Exception("CDCConnection: Unable to activate session: " + getErrorDescription(resultCode));
			}

		} catch (SQLException ex) {
			throw new Exception("CDConnection: Unable to activate session" + ex);
		}
	}

	/**
	 * Stop capturing.
	 * <P>
	 * 
	 * @throws Exception
	 *             if unable to stop capture.
	 */
	public void stopCapture() throws Exception {
		try {
			log.debug("[stopCapture] De-activate CDC session - cdc_deactivatesess");
			CallableStatement cstmt = connection.prepareCall("execute function informix.cdc_deactivatesess(?)");
			cstmt.setInt(1, sessionID);

			ResultSet rs = cstmt.executeQuery();
			rs.next();
			int resultCode = rs.getInt(1);

			if (resultCode != 0) {
				throw new Exception("CDCConnection: Unable to de activate session: " + getErrorDescription(resultCode));
			}
		} catch (SQLException ex) {
			throw new Exception("CDConnection: Unable to de activate session" + ex);
		}
	}

	public Vector<CDCRecord> readData() throws SQLException {
		log.debug("[readData] Get data from SmartBlob - IfxLoRead");
		byte[] b = new byte[READ_DATA_BUFFER_LENGTH]; // TODO: This needs to be tunable..
		IfxSmartBlob smartBlob = new IfxSmartBlob(getSQLConnection());
		int availableBytes = smartBlob.IfxLoRead(getCDCSessionID(), b, b.length); // TODO: concatenate if availableBytes
																					// < recordSize
		return CDCMessageFactory.createCDCRecords(b, availableBytes, this);
	}

	/**
	 * Returns the CDC Session Identifier.
	 * <p>
	 * 
	 * @return the CDC session Identifier
	 */
	public int getCDCSessionID() {
		return sessionID;
	}

	/**
	 * Returns the SQL Connection.
	 * <P>
	 * 
	 * @return the JDBC connection
	 */
	public Connection getSQLConnection() {
		return connection;
	}

	/**
	 * Get a Varchar from a byte buffer.
	 * <P>
	 * The buffer needs to be positioned at the correct place. The buffer position
	 * will also be moved to the size of the Varchar.
	 * <p>
	 * 
	 * @param buffer
	 *            the buffer
	 * @return String representation of the VarChar
	 * @throws Exception
	 *             if unable to get Varchar
	 */
	public static String getVarchar(ByteBuffer buffer) throws Exception {
		try {
			int length = buffer.get();
			byte[] tmp = new byte[length];
			buffer.get(tmp);

			// Single char '\0' is equivalent to a NULL
			if (length == 1 && tmp[0] == '\0') {
				return null;
			}

			return new String(tmp);
		} catch (BufferUnderflowException ex) {
			throw new Exception("BufferUnderFlow exception", ex);
		}
	}

	/**
	 * Get a char from a byte buffer.
	 * <P>
	 * The buffer needs to be positioned at the correct point.
	 * 
	 * @param buffer
	 *            the buffer
	 * @return a single character
	 */
	public static char getChar(ByteBuffer buffer) {
		return (char) buffer.get();
	}

	/**
	 * Get an integer from a byte buffer
	 * <p>
	 * The buffer pointer will be advanced by the size of an integer (4 bytes)
	 * 
	 * @param buffer
	 *            the buffer
	 * @return int value
	 * @throws Exception
	 */
	public static int getInteger(ByteBuffer buffer) throws Exception {
		try {
			return buffer.getInt();
		} catch (BufferUnderflowException ex) {
			throw new Exception("BufferUnderflow exception", ex);
		}
	}

	private String getErrorDescription(int errCode) throws SQLException {
		String errDesc = "Unknow error " + errCode;
		CallableStatement cstmt = connection.prepareCall("select errcode,errname,errdesc from informix.syscdcerrcodes where errcode=?");
		cstmt.setInt(1, errCode);
		ResultSet rs = cstmt.executeQuery();
		if (rs.next()) {
			String errcode = rs.getString(1);
			String errname = rs.getString(2);
			String errdesc = rs.getString(3);
			errDesc = errname + " (" + errcode + ") " + errdesc;
		}
		rs.close();
		cstmt.close();
		return errDesc;
	}
}
package com.conforama.informix.cdc.client;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conforama.informix.cdc.CDCConnection;
import com.conforama.informix.cdc.CDCConnectionDetails;
import com.conforama.informix.cdc.model.CDCMessageFactory;
import com.conforama.informix.cdc.model.common.CDCRecord;
import com.conforama.informix.cdc.model.metadata.CDCMetadataRecord;
import com.conforama.informix.cdc.model.operations.CDCOperationRecord;

@RunWith(Parameterized.class)
public class CDCConnectionTest {

	private static final Logger log = LoggerFactory.getLogger(CDCConnectionTest.class);

	private Map<Integer, CDCMetadataRecord> metadataRecords = new HashMap<Integer, CDCMetadataRecord>();
	private CDCConnection cdcConnection;

	private static final String hostName = "localhost";
	private static final String port = "9088";
	private static final String username = "informix";
	private static final String password = "in4mix";
	private static final String informixServer = "informix";
	private static final String cdcCatalog = "syscdcv1";
	private static final String apiVersion = "1.1";
	private static final int maxRecordsPerReturn = 1;
	private static final int timeout = 0;

	private static final String catalog = "sysadmin";
	private static final String schema = "informix";
	private static final String table = "cdctest";

	private static final List<Column> columns = new LinkedList<CDCConnectionTest.Column>();
	static {
		columns.clear();                                                     
		columns.add(new Column("c_char", 	 "CHAR(10)",					 "some chars"					));
		columns.add(new Column("c_smallint", "SMALLINT",					 (short) 1						));
		columns.add(new Column("c_int", 	 "INTEGER", 					 2								));
		columns.add(new Column("c_float", 	 "FLOAT", 						 3.4							));
		columns.add(new Column("c_smfloat",  "SMALLFLOAT", 					 5.6f							));
		columns.add(new Column("c_decimal",  "DECIMAL", 					 BigDecimal.valueOf(7.8)		));
		columns.add(new Column("c_serial", 	 "SERIAL", 						 0								));
		columns.add(new Column("c_date", 	 "DATE",						 "2018-01-02 00:00:00.000"		));
		columns.add(new Column("c_money", 	 "MONEY",						 BigDecimal.valueOf(125.47)		));
		columns.add(new Column("c_datetime", "DATETIME YEAR TO FRACTION(3)", "2018-01-02 03:04:05.678"		));
		columns.add(new Column("c_varchar",  "VARCHAR(255)", 				 "some varchar"					));
		columns.add(new Column("c_nchar", 	 "NCHAR(20)",					 "some nchar"					));
		columns.add(new Column("c_nvarchar", "NVARCHAR(20)",				 "some nvarchar"				));
		columns.add(new Column("c_int8", 	 "INT8", 						 9								));
		columns.add(new Column("c_serial8",  "SERIAL8", 					 10								));
		columns.add(new Column("c_lvarchar", "LVARCHAR(1024)", 				 "some lvarchar some lvarchar"	));
		columns.add(new Column("c_bool", 	 "BOOLEAN", 					 true							));
		columns.add(new Column("c_bigint", 	 "BIGINT", 						 123456l						));
	}

	@BeforeClass
	public static void setup() throws Exception {
		createTestTable();
	}

	@AfterClass
	public static void teardown() throws SQLException, ClassNotFoundException {
		disableTestTableFullRowLogging();
		dropTestTable();
	}

	@Before
	public void prepare() throws Exception {
		String connectionString = "//" + hostName + ":" + port + "/" + cdcCatalog;
		CDCConnectionDetails connectionDetails = new CDCConnectionDetails(connectionString);
		connectionDetails.setUsername(username);
		connectionDetails.setPassword(password);
		connectionDetails.setInformixServer(informixServer);
		connectionDetails.setMaxRecordsPerReturn(maxRecordsPerReturn);
		connectionDetails.setCdcTimeout(timeout);
		connectionDetails.setInterfaceMajorVersion(Integer.parseInt(apiVersion.split("\\.")[0]));
		connectionDetails.setInterfaceMinorVersion(Integer.parseInt(apiVersion.split("\\.")[1]));
		connectionDetails.setDebugging(false);

		cdcConnection = new CDCConnection(connectionDetails);
		cdcConnection.connect();
	}
	
	
	private String testedColumn;
	
	public CDCConnectionTest(String paramValue) {
		this.testedColumn = paramValue;
	}
	
	@Parameters
	public static Object[] getParameters() {
		List<String> colNames = new LinkedList<String>();
		for (Column column : columns) {
			colNames.add(column.colName);
		}
		return colNames.toArray();
	}
	
	@Test
	public void testCdcConnectionReadData() throws Exception {
		
		log.debug("Enable capture");
		cdcConnection.enableCapture(catalog + ":" + schema + "." + table, testedColumn);
		log.debug("Start capture");
		cdcConnection.startCapture(0);
		insertTestTableRecord();
		List<CDCRecord> cdcRecords;
		boolean exit = false;
		do {
			log.debug("Reading...");
			cdcRecords = cdcConnection.readData();
			if (!cdcRecords.isEmpty()) {
				log.debug("Got " + cdcRecords.size() + " record(s) ("
						+ cdcConnection.getConnectionDetails().getMaxRecordsPerReturn() + "max)");
			}
			for (CDCRecord cdcRecord : cdcRecords) {
				processRecord(cdcRecord);
				if (cdcRecord.isTimeoutRecord()) {
					exit = true;
				}
			}
		} while (!exit);
	}

	private void processRecord(CDCRecord record) {
		if (!record.isTimeoutRecord()) {
			log.debug("{}", record);
		}
		if (record.isMetadataRecord()) {
			CDCMetadataRecord cdcMetadataRecord = (CDCMetadataRecord) record;
			metadataRecords.put(cdcMetadataRecord.getUserData(), cdcMetadataRecord);
		}
		if (record.isOperationalRecord()) {
			CDCOperationRecord cdcOperationRecord = (CDCOperationRecord) record;
			CDCMetadataRecord cdcMetadataRecord = metadataRecords.get(cdcOperationRecord.getUserData());
			Map<String, Object> cdcOperationRecordPayload = CDCMessageFactory.parseCDCRecordPayload(cdcOperationRecord, cdcMetadataRecord);
			for (Entry<String, Object> entry : cdcOperationRecordPayload.entrySet()) {
				String payloadColName = entry.getKey();
				Object payloadValue = entry.getValue();
				Object expectedValue = getColumn(payloadColName).expectedValue;
				if (payloadValue != null && Date.class.isAssignableFrom(payloadValue.getClass())) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					payloadValue = sdf.format(payloadValue);
				}
				try {
					assertEquals(payloadColName, expectedValue, payloadValue);
					log.info("Column '{}'\t Expected: '{}' Got: '{}'", payloadColName, expectedValue, payloadValue);
				} catch (AssertionError e) {
					log.error("Column '{}'\t Expected: '{}' Got: '{}'", payloadColName, expectedValue, payloadValue);
					throw e;
				}
			}
		}
	}

	private static void createTestTable() throws SQLException, ClassNotFoundException {
		Connection rawSqlConnection = getRawSQLConnection(catalog);
		PreparedStatement stmt;
		String sqlStr;
		
		// Build colDeclarations
		List<String> colDeclarations = new LinkedList<String>();
		for (Column column : columns) {
			colDeclarations.add(column.colName + " " + column.colType);
		}
		// Create table
		sqlStr = "create table " + schema + "." + table + "(" + String.join(", ", colDeclarations) + ")";
		log.debug(sqlStr);
		stmt = rawSqlConnection.prepareStatement(sqlStr);
		stmt.execute();
		stmt.close();
		rawSqlConnection.close();
	}

	private static void disableTestTableFullRowLogging() throws SQLException, ClassNotFoundException {
		Connection rawSqlConnection = getRawSQLConnection(cdcCatalog);
		String sqlStr = "execute function informix.cdc_set_fullrowlogging('"+ catalog + ":" + schema + "." + table + "', 0)";
		log.debug(sqlStr);
		PreparedStatement stmt = rawSqlConnection.prepareStatement(sqlStr);
		stmt.execute();
		stmt.close();
		rawSqlConnection.close();
	}

	private static void dropTestTable() throws SQLException, ClassNotFoundException {
		Connection rawSqlConnection = getRawSQLConnection(catalog);
		String _table = catalog + ":" + schema + "." + table; 
		String sqlStr = "drop table " + _table;
		log.debug(sqlStr);
		PreparedStatement stmt = rawSqlConnection.prepareStatement(sqlStr);
		stmt.execute();
		stmt.close();
		rawSqlConnection.close();
	}

	private void insertTestTableRecord() throws SQLException, ClassNotFoundException {
		Connection rawSqlConnection = getRawSQLConnection(catalog);
		
		// Build colDeclarations
		List<String> colDeclarations = new LinkedList<String>();
		for (Column column : columns) {
			colDeclarations.add(column.colName);
		}
		
		// Build colValues
		String[] colValues = new String[columns.size()];
		Arrays.fill(colValues, "?");
		
		// Build sql statement
		String _table = catalog + ":" + schema + "." + table; 
		String sqlStr = "INSERT INTO " + _table + "(" + String.join(", ", colDeclarations) + ") VALUES(" + String.join(", ", colValues) + ")";
		log.debug(sqlStr);
		
		// Set colValues parameters
		PreparedStatement stmt = rawSqlConnection.prepareStatement(sqlStr);
		int i = 0;
		for (Column column : columns) {
			stmt.setObject(++i, column.expectedValue);
		}
		stmt.execute();
		
		stmt.close();
		rawSqlConnection.close();
	}

	private static Connection getRawSQLConnection(String catalog) throws SQLException, ClassNotFoundException {
		Class.forName("com.informix.jdbc.IfxDriver");
		String jdbcURL = "jdbc:informix-sqli://" + hostName + ":" + port + "/" + catalog + ":INFORMIXSERVER=" + informixServer + ";user=" + username + ";password=" + password;
		return DriverManager.getConnection(jdbcURL);
	}
	
	private Column getColumn(String colName) {
		for (Column column : columns) {
			if (column.colName.equalsIgnoreCase(colName)) {
				return column;
			}
		}
		return null;
	}

	static class Column {
		public String colName;
		public String colType;
		public Object expectedValue;

		public Column(String colName, String colType, Object expectedValue) {
			super();
			this.colName = colName;
			this.colType = colType;
			this.expectedValue = expectedValue;
		}
	}

}
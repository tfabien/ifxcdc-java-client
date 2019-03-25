package com.conforama.informix.cdc.model.metadata;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCMetadataRecord extends CDCRecord {

	private int userData;
	private int flags;
	private int fixedLengthSize;
	private int fixedLengthCols;
	private int varLengthCols;
	private String colsCreateStmt;
	private Vector<String> colNames;
	private HashMap<String, String> colTypes;
	private HashMap<String, Integer> colLengths;
	private HashMap<String, Integer> colPrecisions;

	public CDCMetadataRecord(CDCHeader header) {
		super(header);
		parsePayload();
		parseColsCreateStmt();
	}

	public boolean isMetadataRecord() {
		return true;
	}

	@Override
	protected void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();

		userData = buffer.getInt();

		// Must be 0
		flags = buffer.getInt();

		fixedLengthSize = buffer.getInt();

		fixedLengthCols = buffer.getInt();

		varLengthCols = buffer.getInt();
	}

	/**
	 * Parse the payload
	 */
	private void parsePayload() {
		ByteBuffer payload = getPayload();

		byte[] tmp = new byte[payload.capacity()];
		payload.get(tmp);
		this.colsCreateStmt = new String(tmp);
	}

	private void parseColsCreateStmt() {
		colNames = new Vector<String>();
		colTypes = new LinkedHashMap<String, String>();
		colLengths = new LinkedHashMap<String, Integer>();
		colPrecisions = new LinkedHashMap<String, Integer>();
		Pattern pattern = Pattern.compile("(\\w+)\\s+(?:([\\w\\s]+)(?:\\(([^)]*)\\))?)[,$]?", Pattern.MULTILINE | Pattern.DOTALL);
		Matcher matcher = pattern.matcher(this.colsCreateStmt);
		while (matcher.find()) {
			String colName = matcher.group(1);
			colNames.add(colName);
			colTypes.put(colName, matcher.group(2));
			if (matcher.groupCount() > 2 && matcher.group(3) != null) {
				String lenPrec = matcher.group(3);
				if (lenPrec.contains(",")) {
					colLengths.put(colName, Integer.parseInt(lenPrec.split(",")[0]));
					colPrecisions.put(colName, Integer.parseInt(lenPrec.split(",")[1]));
				} else {
					colLengths.put(colName, Integer.parseInt(lenPrec));
					colPrecisions.put(colName, null);
				}
			} else {
				colLengths.put(colName, null);
				colPrecisions.put(colName, null);
			}
		}
	}

	@Override
	public boolean hasUserData() {
		return true;
	}
	
	public int getUserData() {
		return userData;
	}

	public int getFlags() {
		return flags;
	}

	public int getFixedLengthSize() {
		return fixedLengthSize;
	}

	public int getFixedLengthCols() {
		return fixedLengthCols;
	}

	public int getVarLengthCols() {
		return varLengthCols;
	}

	public String getColsCreateStmt() {
		return colsCreateStmt;
	}

	public Vector<String> getColNames() {
		return colNames;
	}
	
	public HashMap<String, String> getColTypes() {
		return colTypes;
	}

	public HashMap<String, Integer> getColLengths() {
		return colLengths;
	}

	public HashMap<String, Integer> getColPrecisions() {
		return colPrecisions;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + " " + ToStringBuilder.reflectionToString(this, ToStringStyle.NO_CLASS_NAME_STYLE);
	}

}
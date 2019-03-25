package com.conforama.informix.cdc.model.metadata;

import java.nio.ByteBuffer;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCErrorRecord extends CDCRecord {

	private int flag;
	private int errorCode;

	/**
	 * Constructor
	 * 
	 * @param header
	 */
	public CDCErrorRecord(CDCHeader header) {
		super(header);
	}

	public boolean isErrorRecord() {
		return true;
	}
	
	@Override
	protected void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();
		flag = buffer.getInt();
		errorCode = buffer.getInt();
	}

	public boolean isSessionValid() {
		return (flag != 1);
	}

	public int getFlag() {
		return flag;
	}

	public int getErrorCode() {
		return errorCode;
	}
	
	@Override
	public String toString() {
		return "CDCErrorRecord: Flag [" + flag + "] Error Code [" + errorCode + "]";
	}

}
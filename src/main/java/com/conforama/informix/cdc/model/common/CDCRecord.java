package com.conforama.informix.cdc.model.common;

import java.nio.ByteBuffer;

import com.conforama.informix.cdc.CDCConstants;

public abstract class CDCRecord {
	
	private final CDCHeader header;

	public CDCRecord(CDCHeader header) {
		this.header = header;
		parseRecordSpecificHeader();
	}
	
	protected abstract void parseRecordSpecificHeader();
	
	public int getSize() {
		return header.getHeaderSize() + header.getPayloadSize() + CDCConstants.CDC_HEADER_SIZE;
	}

	public final int getCDCRecordType() {
		return header.getLogRecordType();
	}

	public final ByteBuffer getPayload() {
		return (ByteBuffer) header.getPayload().rewind();
	}

	public final ByteBuffer getHeader() {
		return (ByteBuffer) header.getHeader().rewind();
	}

	public boolean isBeginTransactionRecord() {
		return false;
	}

	public boolean isCommitRecord() {
		return false;
	}

	public boolean isDeleteRecord() {
		return false;
	}

	public boolean isDiscardRecord() {
		return false;
	}

	public boolean isErrorRecord() {
		return false;
	}

	public boolean isInsertRecord() {
		return false;
	}

	public boolean isRollbackRecord() {
		return false;
	}

	public boolean isMetadataRecord() {
		return false;
	}

	public boolean isTimeoutRecord() {
		return false;
	}

	public boolean isTruncateRecord() {
		return false;
	}

	public boolean isUpdateBeforeRecord() {
		return false;
	}

	public boolean isUpdateAfterRecord() {
		return false;
	}

	public boolean hasUserData() {
		return false;
	}	

	public int getUserData() {
		return 0;
	}

	public boolean isOperationalRecord() {
		return false;
	}

}
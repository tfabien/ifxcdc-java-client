package com.conforama.informix.cdc.model.operations.truncate;

import java.nio.ByteBuffer;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCTruncateRecord extends CDCRecord {

	private long sequenceNumber;
	private int transactionID;
	private int userData;

	public CDCTruncateRecord(CDCHeader header) {
		super(header);
	}

	public boolean isTruncateRecord() {
		return false;
	}

	@Override
	protected void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();
		sequenceNumber = buffer.getLong();
		transactionID = buffer.getInt();
		userData = buffer.getInt();
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public int getTransactionID() {
		return transactionID;
	}

	public int getUserData() {
		return userData;
	}

	@Override
	public boolean hasUserData() {
		return true;
	}

	@Override
	public String toString() {
		return "CDCTruncateRecord: Sequence Number [" + sequenceNumber + "] Transaction ID [" + transactionID + "] User Data [" + userData + "]";
	}

}

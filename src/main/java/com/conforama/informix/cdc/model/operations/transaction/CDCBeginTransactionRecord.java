package com.conforama.informix.cdc.model.operations.transaction;

import java.nio.ByteBuffer;
import java.util.Date;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCBeginTransactionRecord extends CDCRecord {

	private long sequenceNumber;
	private int transactionID;
	private long startTime;
	private Date startDate = null;
	private int userID;

	public CDCBeginTransactionRecord(CDCHeader header) {
		super(header);
	}

	public boolean isBeginTransactionRecord() {
		return true;
	}

	@Override
	protected void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();
		sequenceNumber = buffer.getLong();
		transactionID = buffer.getInt();
		startTime = buffer.getLong();
		userID = buffer.getInt();
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public int getTransactionID() {
		return transactionID;
	}

	public long getStartTime() {
		return startTime;
	}

	public Date getStartDate() {
		if (startDate == null) {
			startDate = new Date(startTime);
		}
		return startDate;
	}

	public int getUserID() {
		return userID;
	}

	@Override
	public String toString() {
		return "CDCBeginTransactionRecord: Sequence Number [" + sequenceNumber + "] Transaction ID [" + transactionID + "] Start Time [" + startTime + "] User ID [" + userID + "]";
	}

}
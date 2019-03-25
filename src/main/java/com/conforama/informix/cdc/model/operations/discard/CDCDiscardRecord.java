package com.conforama.informix.cdc.model.operations.discard;

import java.nio.ByteBuffer;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCDiscardRecord extends CDCRecord {

	private long sequenceNumber;
	private int transactionID;

	public CDCDiscardRecord(CDCHeader header) {
		super(header);
	}

	public boolean isDiscardRecord() {
		return true;
	}

	@Override
	protected void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();
		sequenceNumber = buffer.getLong();
		transactionID = buffer.getInt();
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public int getTransactionID() {
		return transactionID;
	}

	@Override
	public String toString() {
		return "CDCDiscardRecord: SequenceNumber [" + sequenceNumber + "] Transaction ID [" + transactionID + "]";
	}

}
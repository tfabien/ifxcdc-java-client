package com.conforama.informix.cdc.model.operations.transaction;

import java.nio.ByteBuffer;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCRollBackRecord extends CDCRecord {

	private long sequenceNumber;
	private int transactionID;

	public CDCRollBackRecord(CDCHeader header) {
		super(header);
	}

	public boolean isRollbackRecord() {
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
		return "CDCRollbackRecord: SequenceNumber [" + sequenceNumber + "] Transaction ID [" + transactionID + "]";
	}

}
package com.conforama.informix.cdc.model.operations;

import java.nio.ByteBuffer;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public abstract class CDCOperationRecord extends CDCRecord {

	private long sequenceNumber;
	private int transactionID;
	private int userData;
	private int flags;

	public CDCOperationRecord(CDCHeader header) {
		super(header);
	}

	@Override
	public final void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();
		sequenceNumber = buffer.getLong();
		transactionID = buffer.getInt();
		userData = buffer.getInt();
		flags = buffer.getInt();
	}


	public final long getSequenceNumber() {
		return sequenceNumber;
	}

	public final int getTransactionID() {
		return transactionID;
	}

	@Override
	public final int getUserData() {
		return userData;
	}

	public final int getFlags() {
		return flags;
	}

	@Override
	public final boolean hasUserData() {
		return true;
	}

	@Override
	public final boolean isOperationalRecord() {
		return true;
	}
	
	@Override
	public String toString() {
		return " Sequence Number [" + sequenceNumber + "] Transaction ID [" + transactionID + "] User Data [" + userData + "]";
	}

}
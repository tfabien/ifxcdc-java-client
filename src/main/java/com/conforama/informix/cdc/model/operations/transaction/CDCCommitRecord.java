package com.conforama.informix.cdc.model.operations.transaction;

import java.nio.ByteBuffer;
import java.util.Date;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCCommitRecord extends CDCRecord {

	private long sequenceNumber;
	private int transactionID;
	private long commitTime;
	private Date commitDate = null;

	public CDCCommitRecord(CDCHeader header) {
		super(header);
	}

	public boolean isCommitRecord() {
		return true;
	}

	@Override
	protected void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();
		sequenceNumber = buffer.getLong();
		transactionID = buffer.getInt();
		commitTime = buffer.getLong();
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public int getTransactionID() {
		return transactionID;
	}

	public long getCommitTime() {
		return commitTime;
	}

	public Date getCommitDate() {
		if (commitDate == null) {
			commitDate = new Date(commitTime);
		}
		return commitDate;
	}

	@Override
	public String toString() {
		return "CDCCommitRecord: Sequence Number [" + sequenceNumber + "] Transaction ID [" + transactionID + "] Time [" + commitTime + "]";
	}

}
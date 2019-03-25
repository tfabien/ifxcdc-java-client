package com.conforama.informix.cdc.model.metadata;

import java.nio.ByteBuffer;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;

public class CDCTimeoutRecord extends CDCRecord {

	private long sequenceNumber;

	public CDCTimeoutRecord(CDCHeader header) {
		super(header);
	}

	public boolean isTimeoutRecord() {
		return true;
	}

	@Override
	protected void parseRecordSpecificHeader() {
		ByteBuffer buffer = getHeader();
		sequenceNumber = buffer.getLong();
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public String toString() {
		return "CDCTimoutRecord: " + sequenceNumber;
	}

}

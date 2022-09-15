package com.conforama.informix.cdc.model.operations.insert;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.operations.CDCOperationRecord;

public class CDCInsertRecord extends CDCOperationRecord {

	public CDCInsertRecord(CDCHeader header) {
		super(header);
	}

	@Override
	public boolean isInsertRecord() {
		return true;
	}

	@Override
	public String toString() {
		return "CDCInsertRecord: " + super.toString();
	}

}
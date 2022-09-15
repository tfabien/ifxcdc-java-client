package com.conforama.informix.cdc.model.operations.delete;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.operations.CDCOperationRecord;

public class CDCDeleteRecord extends CDCOperationRecord {

	public CDCDeleteRecord(CDCHeader header) {
		super(header);
	}

	@Override
	public boolean isDeleteRecord() {
		return true;
	}

	@Override
	public String toString() {
		return "CDCDeleteRecord: " + super.toString();
	}

}
package com.conforama.informix.cdc.model.operations.update;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.operations.CDCOperationRecord;

public class CDCUpdateAfterRecord extends CDCOperationRecord {

	public CDCUpdateAfterRecord(CDCHeader header) {
		super(header);
	}
	public boolean isUpdateAfterRecord() {
		return true;
	}

	@Override
	public String toString() {
		return "CDCUpdateAfterRecord: " + super.toString();
	}

}
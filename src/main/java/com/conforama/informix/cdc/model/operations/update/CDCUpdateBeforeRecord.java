package com.conforama.informix.cdc.model.operations.update;

import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.operations.CDCOperationRecord;

public class CDCUpdateBeforeRecord extends CDCOperationRecord {

	public CDCUpdateBeforeRecord(CDCHeader header) {
		super(header);
	}

	public boolean isUpdateBeforeRecord() {
		return true;
	}

	@Override
	public String toString() {
		return "CDCUpdateBeforeRecord: " + super.toString();
	}
}

package com.conforama.informix.cdc;

import com.conforama.informix.cdc.model.common.CDCRecord;

/**
 * The interface for receiving events from a {@link CDCConnection}.
 * <P>
 * Such a listener may be added using the
 * {@link CDCConnection#addCDCRecordListener(CDCRecordListener)} method.
 * 
 * @author pwalsh
 * 
 */
public interface CDCRecordListener {

	/**
	 * This method will be called when a CDC record has arrived.
	 * <p>
	 * 
	 * @param record
	 *            a CDC Record
	 */
	public void onCDCRecord(CDCRecord record);

}
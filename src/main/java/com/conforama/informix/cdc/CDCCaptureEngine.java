package com.conforama.informix.cdc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conforama.informix.cdc.model.common.CDCRecord;
import com.informix.jdbc.IfxSmartBlob;

public final class CDCCaptureEngine implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(CDCCaptureEngine.class);

	private boolean isRunning = false;
	private CDCConnection cdcConnection;
	private IfxSmartBlob smartBlob = null;
	private Thread thread;
	private List<CDCRecordListener> listeners;

	/**
	 * Constructor
	 * 
	 * @param connection
	 */
	public CDCCaptureEngine(CDCConnection connection) {
		cdcConnection = connection;
		listeners = new ArrayList<CDCRecordListener>();
	}

	/**
	 * start
	 * @throws Exception 
	 * 
	 */
	public void start() throws Exception {
		cdcConnection.startCapture(0);
		
		isRunning = true;
		thread = new Thread(this, "CDCCaptureEngine");
		thread.start();
	}

	/**
	 * stop
	 * @throws Exception 
	 */
	public void stop() throws Exception {
		isRunning = false;

		if (smartBlob != null) {
			smartBlob.notify();
		}
		
		cdcConnection.stopCapture();
	}

	/**
	 * @see com.pushtechnology.diffusion.api.threads.RunnableTask#run()
	 */
	public void run() {

		while (isRunning) {
			try {
				// Blocking ...
				Vector<CDCRecord> records = cdcConnection.readData();
				if (records != null) {
					for (CDCRecord cdcRecord : records) {
						onCDCRecord(cdcRecord);						
					}					
				}
			} catch (SQLException ex) {
				isRunning = false;
				log.warn("CDCCaptureEngine: problem with CDC capture", ex);
			}
		}
	}

	/**
	 * @see com.conforama.informix.cdc.client.model.CDCRecordListener#onCDCRecord(com.conforama.informix.cdc.model.CDCRecord)
	 */
	public void onCDCRecord(CDCRecord record) {
		for (CDCRecordListener listeners : listeners) {
			try {
				listeners.onCDCRecord(record);
			} catch (Throwable t) {
				log.warn("CDCConnection: Exception caugth in onCDCRecord", t);
			}
		}
	}

	/**
	 * Add a CDC record listener
	 * <p>
	 * 
	 * @param listener
	 *            the listener to add
	 */
	public void addCDCRecordListener(CDCRecordListener listener) {
		listeners.add(listener);
	}

	/**
	 * Remove CDC record listener
	 * <p>
	 * 
	 * @param listener
	 *            the listener to remove
	 */
	public void removeCDCRecordListener(CDCRecordListener listener) {
		listeners.remove(listener);
	}

}

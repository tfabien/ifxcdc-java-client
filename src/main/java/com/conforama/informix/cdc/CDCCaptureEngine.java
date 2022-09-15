package com.conforama.informix.cdc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.conforama.informix.cdc.model.common.CDCRecord;
import com.informix.jdbc.IfxSmartBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CDCCaptureEngine implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(CDCCaptureEngine.class);
    private final CDCConnection cdcConnection;
    private final IfxSmartBlob smartBlob = null;
    private final List<CDCRecordListener> listeners;
    private boolean isRunning = false;
    private Thread thread;

    /**
     * Constructor
     *
     * @param connection
     */
    public CDCCaptureEngine(CDCConnection connection) {
        cdcConnection = connection;
        listeners = new ArrayList<>();
    }

    /**
     * start
     *
     * @throws Exception
     */
    public void start() throws Exception {
        cdcConnection.startCapture(0);

        isRunning = true;
        thread = new Thread(this, "CDCCaptureEngine");
        thread.start();
    }

    /**
     * stop
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        isRunning = false;

        if (smartBlob != null) {
            smartBlob.notify();
        }

        cdcConnection.stopCapture();
    }


    public void run() {

        while (isRunning) {
            try {
                // Blocking ...
                List<CDCRecord> records = cdcConnection.readData();
                for (CDCRecord cdcRecord : records) {
                    onCDCRecord(cdcRecord);
                }
            } catch (SQLException ex) {
                isRunning = false;
                log.warn("CDCCaptureEngine: problem with CDC capture", ex);
            }
        }
    }

    public void onCDCRecord(CDCRecord record) {
        for (CDCRecordListener listener : listeners) {
            try {
                listener.onCDCRecord(record);
            } catch (Throwable t) {
                log.warn("CDCConnection: Exception caugth in onCDCRecord", t);
            }
        }
    }

    /**
     * Add a CDC record listener
     * <p>
     *
     * @param listener the listener to add
     */
    public void addCDCRecordListener(CDCRecordListener listener) {
        listeners.add(listener);
    }

    /**
     * Remove CDC record listener
     * <p>
     *
     * @param listener the listener to remove
     */
    public void removeCDCRecordListener(CDCRecordListener listener) {
        listeners.remove(listener);
    }

}

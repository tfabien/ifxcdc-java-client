package com.conforama.informix.cdc.model;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.conforama.informix.cdc.CDCConnection;
import com.conforama.informix.cdc.CDCConstants;
import com.conforama.informix.cdc.model.common.CDCHeader;
import com.conforama.informix.cdc.model.common.CDCRecord;
import com.conforama.informix.cdc.model.metadata.CDCErrorRecord;
import com.conforama.informix.cdc.model.metadata.CDCMetadataRecord;
import com.conforama.informix.cdc.model.metadata.CDCTimeoutRecord;
import com.conforama.informix.cdc.model.operations.CDCOperationRecord;
import com.conforama.informix.cdc.model.operations.delete.CDCDeleteRecord;
import com.conforama.informix.cdc.model.operations.discard.CDCDiscardRecord;
import com.conforama.informix.cdc.model.operations.insert.CDCInsertRecord;
import com.conforama.informix.cdc.model.operations.transaction.CDCBeginTransactionRecord;
import com.conforama.informix.cdc.model.operations.transaction.CDCCommitRecord;
import com.conforama.informix.cdc.model.operations.transaction.CDCRollBackRecord;
import com.conforama.informix.cdc.model.operations.truncate.CDCTruncateRecord;
import com.conforama.informix.cdc.model.operations.update.CDCUpdateAfterRecord;
import com.conforama.informix.cdc.model.operations.update.CDCUpdateBeforeRecord;
import com.google.common.primitives.Shorts;
import com.informix.lang.IfxToJavaType;
import com.informix.lang.IfxTypes;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDCMessageFactory {

    private static final Logger log = LoggerFactory.getLogger(CDCMessageFactory.class);
    private static final int IFX_TYPE_DATETIME_BYTES_LEN = 8;

    public static List<CDCRecord> createCDCRecords(byte[] data, int availableBytes, CDCConnection cdcConnection) {
        LinkedList<CDCRecord> records = new LinkedList<>();
        int index = 0;
        while (index < availableBytes) {
            byte[] b = Arrays.copyOfRange(data, index, availableBytes);
            CDCRecord cdcRecord = createCDCRecord(b);
            records.add(cdcRecord);
            index += cdcRecord.getSize();
        }
        return records;
    }

    public static CDCRecord createCDCRecord(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int length = buffer.getInt() + buffer.getInt();
        CDCHeader header = new CDCHeader(data, length);

        switch (header.getLogRecordType()) {
            case CDCConstants.CDC_REC_TABSCHEMA:
                return new CDCMetadataRecord(header);

            case CDCConstants.CDC_REC_UPDBEF:
                return new CDCUpdateBeforeRecord(header);

            case CDCConstants.CDC_REC_UPDAFT:
                return new CDCUpdateAfterRecord(header);

            case CDCConstants.CDC_REC_TIMEOUT:
                return new CDCTimeoutRecord(header);

            case CDCConstants.CDC_REC_INSERT:
                return new CDCInsertRecord(header);

            case CDCConstants.CDC_REC_DELETE:
                return new CDCDeleteRecord(header);

            case CDCConstants.CDC_REC_BEGINTX:
                return new CDCBeginTransactionRecord(header);

            case CDCConstants.CDC_REC_COMMTX:
                return new CDCCommitRecord(header);

            case CDCConstants.CDC_REC_DISCARD:
                return new CDCDiscardRecord(header);

            case CDCConstants.CDC_REC_ERROR:
                return new CDCErrorRecord(header);

            case CDCConstants.CDC_REC_RBTX:
                return new CDCRollBackRecord(header);

            case CDCConstants.CDC_REC_TRUNCATE:
                return new CDCTruncateRecord(header);

            default:
        }
        return null;
    }

    public static Map<String, Object> parseCDCRecordPayload(CDCOperationRecord cdcOperationRecord, CDCMetadataRecord cdcMetadataRecord) {
        if (cdcOperationRecord.getUserData() != cdcMetadataRecord.getUserData()) {
            log.warn("cdcOperationRecord userData does not match cdcMetadataRecord userData");
            return new HashMap<>();
        }
        int totalCols = cdcMetadataRecord.getFixedLengthCols() + cdcMetadataRecord.getVarLengthCols();
        Map<String, Object> map = new HashMap<>(totalCols);
        ByteBuffer payload = cdcOperationRecord.getPayload();

        // Parse Fixed length cols
        for (int i = 0; i < cdcMetadataRecord.getFixedLengthCols(); i++) {
            String colName = cdcMetadataRecord.getColNames().get(i);
            String colType = cdcMetadataRecord.getColTypes().get(colName);
            Integer colLength = cdcMetadataRecord.getColLengths().get(colName);
            Integer colPrec = cdcMetadataRecord.getColPrecisions().get(colName);
            Object colValue;
            try {
                colValue = parseFixedLengthCol(payload, colType, colLength, colPrec);
            } catch (Exception e) {
                colValue = e.getMessage();
            }
            log.debug("{} ({}): {}", colName, colType, colValue);
            map.put(colName, colValue);
        }

        // Parse Variable length cols
        payload.position(cdcMetadataRecord.getFixedLengthSize());
        for (int i = cdcMetadataRecord.getFixedLengthCols(); i < totalCols; i++) {
            String colName = cdcMetadataRecord.getColNames().get(i);
            String colType = cdcMetadataRecord.getColTypes().get(colName);
            Object colValue;
            try {
                colValue = parseVariableLengthCol(payload, colType);
            } catch (Exception e) {
                colValue = e.getMessage();
            }
            log.debug("{} ({}): {}", colName, colType, colValue);
            map.put(colName, colValue);
        }
        return map;
    }

    private static Object parseFixedLengthCol(ByteBuffer payload, String colType, Integer colLength, Integer colPrecision) {

        int ifxType = IfxTypes.FromIfxNameToIfxType(colType.split(" ")[0]);

        if (IfxTypes.IFX_TYPE_SERIAL == ifxType) {
            return payload.getInt();
        }

        if (IfxTypes.IFX_TYPE_INT == ifxType) {
            return payload.getInt();
        }

        if (IfxTypes.IFX_TYPE_DATE == ifxType) {
            return new Date(payload.getLong());
        }

        if (IfxTypes.IFX_TYPE_BOOL == ifxType) {
            byte isNull = payload.get();
            byte value = payload.get();
            return isNull == 1 ? null : value == 1;
        }

        if (IfxTypes.IFX_TYPE_CHAR == ifxType || IfxTypes.IFX_TYPE_NCHAR == ifxType) {
            byte[] b = new byte[colLength];
            payload.get(b);
            return new String(b).trim();
        }

        if (IfxTypes.IFX_TYPE_FLOAT == ifxType) {
            return payload.getDouble();
        }

        if (IfxTypes.IFX_TYPE_SMALLINT == ifxType) {
            return payload.getShort();
        }

        if (IfxTypes.IFX_TYPE_SMFLOAT == ifxType) {
            return payload.getFloat();
        }

        if (IfxTypes.IFX_TYPE_DECIMAL == ifxType || IfxTypes.IFX_TYPE_MONEY == ifxType) {
            byte[] b = new byte[IFX_TYPE_DATETIME_BYTES_LEN];
            payload.get(b);
            return IfxToJavaType.IfxToJavaDecimal(b, colPrecision != null ? (short) colPrecision.intValue() : (short) 0);
        }

        if (IfxTypes.IFX_TYPE_DATETIME == ifxType) {
            try {
                byte[] b = new byte[IFX_TYPE_DATETIME_BYTES_LEN];
                payload.get(b);
                BigDecimal decimal = IfxToJavaType.IfxToJavaDecimal(b, colPrecision != null ? (short) colPrecision.intValue() : (short) 0);
                String decimalStr = decimal.toString();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
                decimalStr = decimalStr.length() > 14 ? decimalStr : decimalStr + ".000";
                return sdf.parse(decimalStr);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        throw new NotImplementedException("Unsupported IfxType: " + ifxType + " (" + colType + ")");
    }

    private static Object parseVariableLengthCol(ByteBuffer payload, String colType) {

        int ifxType = IfxTypes.FromIfxNameToIfxType(colType.split(" ")[0]);

        if (IfxTypes.IFX_TYPE_NVCHAR == ifxType || IfxTypes.IFX_TYPE_VARCHAR == ifxType) {
            int length = payload.get();
            byte[] b = new byte[length];
            payload.get(b);
            return length == 1 && b[0] == '\0' ? null : new String(b);
        }

        if (IfxTypes.IFX_TYPE_LVARCHAR == ifxType) {
            byte[] lb = new byte[3];
            payload.get(lb);
            int length = Shorts.fromByteArray(lb) - 1;
            byte[] b = new byte[length];
            payload.get(b);
            return length == 1 && b[0] == '\0' ? null : new String(b);
        }

        throw new NotImplementedException("Unsupported IfxType: " + ifxType + " (" + colType + ")");
    }
}

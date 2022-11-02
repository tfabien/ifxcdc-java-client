/*
 * Decompiled with CFR 0_132.
 */
package com.informix.lang;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import com.informix.util.IfxErrMsg;

public final class _IfxToJavaType {
    public static final int DAYS1900 = 693594;
    public static final int DAYS4CENT = 146097;
    public static final int DAYS4YEAR = 1461;
    private static final ThreadLocal<Calendar> localCalendar = new ThreadLocal<Calendar>() {

        @Override
        protected Calendar initialValue() {
            return Calendar.getInstance();
        }
    };
    private static final ThreadLocal<Map<String, CharsetDecoder>> localDecoder = new ThreadLocal<Map<String, CharsetDecoder>>() {

        @Override
        protected Map<String, CharsetDecoder> initialValue() {
            return new HashMap<>();
        }
    };

    private static String normalizeDbEncString(String dbEnc) {
        if (dbEnc == null || dbEnc.equalsIgnoreCase("NOENCODING")) {
            return "ISO8859_1";
        }
        if (dbEnc.equalsIgnoreCase("ISO2022CN_GB")) {
            return "ISO2022CN";
        }
        return dbEnc;
    }

    private static CharsetDecoder getCharsetDecoder(String dbEnc) {
        Map<String, CharsetDecoder> decoderList = localDecoder.get();
        if (dbEnc == null) {
            dbEnc = _IfxToJavaType.normalizeDbEncString(dbEnc);
        }
        if (decoderList.containsKey(dbEnc)) {
            return decoderList.get(dbEnc);
        }
        CharsetDecoder decoder = Charset.forName(dbEnc).newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPORT);
        decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        decoderList.put(dbEnc, decoder);
        return decoder;
    }

    public static String IfxToJavaChar(byte[] b, int offset, int length, String dbEnc, boolean encoption)
            throws IOException {
        String normalizedDbEnc = _IfxToJavaType.normalizeDbEncString(dbEnc);
        if (encoption) {
            return new String(b, offset, length, normalizedDbEnc);
        }
        if (normalizedDbEnc.equals("NOENCODING")) {
            return new String(b, offset, length);
        }
        ByteBuffer bbuf = ByteBuffer.wrap(b, offset, length);
        try {
            CharsetDecoder cdec = _IfxToJavaType.getCharsetDecoder(normalizedDbEnc);
            CharBuffer res = cdec.decode(bbuf);
            String s = res.toString();
            if (s.length() == 0 && length == 1) {
                bbuf = ByteBuffer.allocate(length + 1);
                bbuf.put(b, offset, length);
                bbuf.position(0);
                s = cdec.decode(bbuf).toString().substring(0, length);
            }
            return s;
        } catch (CharacterCodingException e) {
            throw new IOException(IfxErrMsg.getMessage(-23103));
        }
    }

    public static Date IfxToJavaDate(byte[] b) {
        return _IfxToJavaType.IfxToJavaDate(b, 0);
    }

    public static Date IfxToJavaDate(byte[] b, int offset) {
        int dt = _IfxToJavaType.IfxToJavaInt(b, offset);
        if (dt == Integer.MIN_VALUE) {
            return null;
        }
        return _IfxToJavaType.convertDaysToDate(dt);
    }

    public static boolean rleapyear(int yr) {
        return (yr & 3) == 0 && (yr % 400 == 0 || yr % 100 != 0);
    }

    public static Date convertDaysToDate(int dt) {
        int mon;
        int jdate = dt + 693594;
        byte[] daymon_val = new byte[]{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int lyear = (jdate * 4 + 3) / 146097;
        int year = lyear * 100;
        lyear = ((jdate -= lyear * 146097 / 4) * 4 + 3) / 1461;
        int day = (jdate -= lyear * 1461 / 4) + 1;
        daymon_val[2] = (byte) (_IfxToJavaType.rleapyear(year += lyear + 1) ? 29 : 28);
        for (mon = 1; mon <= 12 && day > daymon_val[mon]; day -= daymon_val[mon], ++mon) {
        }
        GregorianCalendar gc = (GregorianCalendar) localCalendar.get();
        gc.set(year, mon - 1, day, 0, 0, 0);
        gc.set(14, 0);
        Date d = new Date(gc.getTimeInMillis());
        return d;
    }

    public static int convertDateToDays(Date dt) {
        byte[] daymon_val = new byte[]{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        Calendar cal_val = Calendar.getInstance();
        cal_val.setTime(dt);
        int mon = cal_val.get(2) + 1;
        int day = cal_val.get(5);
        int year = cal_val.get(1);
        daymon_val[2] = (byte) (_IfxToJavaType.rleapyear(year) ? 29 : 28);
        int lyear = year - 1;
        int jdate = lyear / 100 * 146097 / 4 + lyear % 100 * 1461 / 4 + day - 1 - 693594;
        for (int i = 1; i < mon; ++i) {
            jdate += daymon_val[i];
        }
        return jdate;
    }

    public static Timestamp IfxToJavaDateTime(byte[] b, short prec) {
        return _IfxToJavaType.IfxToJavaDateTime(b, 0, b.length, prec, null);
    }

    public static Timestamp IfxToJavaDateTime(byte[] b, int offset, int length, short prec) {
        return _IfxToJavaType.IfxToJavaDateTime(b, offset, length, prec, null);
    }

    public static Timestamp IfxToJavaDateTime(byte[] b, int offset, int length, short prec, Calendar cal) {
        Decimal d = new Decimal(b, offset, length, prec, true);
        return d.timestampValue(cal);
    }

    public static String IfxToDateTimeUnloadString(byte[] b, int offset, int length, short prec) {
        Decimal d = new Decimal(b, offset, length, prec, true);
        return d.timestampStringValue();
    }

    public static Interval IfxToJavaInterval(byte[] b, short prec) {
        return _IfxToJavaType.IfxToJavaInterval(b, 0, b.length, prec);
    }

    public static Interval IfxToJavaInterval(byte[] b, int offset, int length, short prec) {
        Decimal d = new Decimal(b, offset, length, prec, true);
        if (d.dec_pos == -1) {
            return null;
        }
        return d.intervalValue();
    }

    public static BigDecimal IfxToJavaDecimal(byte[] b, short prec) {
        return _IfxToJavaType.IfxToJavaDecimal(b, 0, b.length, prec);
    }

    public static BigDecimal IfxToJavaDecimal(byte[] b, int offset, int length, short prec) {
        Decimal d = new Decimal(b, offset, length, prec);
        return d.numericValue();
    }

    public static double IfxToJavaDouble(byte[] b) {
        return _IfxToJavaType.IfxToJavaDouble(b, 0);
    }

    public static double IfxToJavaDouble(byte[] b, int offset) {
        long val = (long) b[offset] << 56 | (long) b[1 + offset] << 48 & 0xFF000000000000L
                | (long) b[2 + offset] << 40 & 0xFF0000000000L | (long) b[3 + offset] << 32 & 0xFF00000000L
                | (long) b[4 + offset] << 24 & 0xFF000000L | (long) b[5 + offset] << 16 & 0xFF0000L
                | (long) b[6 + offset] << 8 & 65280L | (long) b[7 + offset] & 255L;
        return Double.longBitsToDouble(val);
    }

    public static float IfxToJavaReal(byte[] b) {
        return _IfxToJavaType.IfxToJavaReal(b, 0);
    }

    public static float IfxToJavaReal(byte[] b, int offset) {
        int val = _IfxToJavaType.IfxToJavaInt(b, offset);
        return Float.intBitsToFloat(val);
    }

    private static short widenByte(byte b) {
        return (short) ((short) b & 255);
    }

    public static short IfxToJavaSmallInt(byte[] b) {
        return _IfxToJavaType.IfxToJavaSmallInt(b, 0);
    }

    public static short IfxToJavaSmallInt(byte[] b, int offset) {
        short s = b[offset];
        s = (short) ((s << 8) + _IfxToJavaType.widenByte(b[1 + offset]));
        return s;
    }

    public static int IfxToJavaInt(byte[] b) {
        return _IfxToJavaType.IfxToJavaInt(b, 0);
    }

    public static int IfxToJavaInt(byte[] b, int offset) {
        int i = b[offset];
        i = (((i << 8) + _IfxToJavaType.widenByte(b[offset + 1]) << 8) + _IfxToJavaType.widenByte(b[offset + 2]) << 8)
                + _IfxToJavaType.widenByte(b[offset + 3]);
        return i;
    }

    public static long IfxToJavaLongInt(byte[] b) {
        return _IfxToJavaType.IfxToJavaLongInt(b, 0);
    }

    public static long IfxToJavaLongInt(byte[] buf, int offset) {
        long l = Long.MIN_VALUE;
        short sign = (short) ((buf[offset] << 8) + _IfxToJavaType.widenByte(buf[offset + 1]));
        if (sign != 0) {
            l = 255 & buf[offset + 6];
            l = l << 8 | (long) (255 & buf[offset + 7]);
            l = l << 8 | (long) (255 & buf[offset + 8]);
            l = l << 8 | (long) (255 & buf[offset + 9]);
            l = l << 8 | (long) (255 & buf[offset + 2]);
            l = l << 8 | (long) (255 & buf[offset + 3]);
            l = l << 8 | (long) (255 & buf[offset + 4]);
            l = l << 8 | (long) (255 & buf[offset + 5]);
            if (sign == -1) {
                l *= -1L;
            }
        }
        return l;
    }

    public static long IfxToJavaLongBigInt(byte[] b) {
        return _IfxToJavaType.IfxToJavaLongBigInt(b, 0);
    }

    public static long IfxToJavaLongBigInt(byte[] b, int offset) {
        long l = b[offset];
        l = (((((((l << 8) + (long) _IfxToJavaType.widenByte(b[offset + 1]) << 8)
                + (long) _IfxToJavaType.widenByte(b[offset + 2]) << 8)
                + (long) _IfxToJavaType.widenByte(b[offset + 3]) << 8)
                + (long) _IfxToJavaType.widenByte(b[offset + 4]) << 8)
                + (long) _IfxToJavaType.widenByte(b[offset + 5]) << 8)
                + (long) _IfxToJavaType.widenByte(b[offset + 6]) << 8) + (long) _IfxToJavaType.widenByte(b[offset + 7]);
        return l;
    }

    public String IfxToJavaChar(byte[] b, boolean encoption) throws IOException {
        int adjustedLength = 0;
        return this.IfxToJavaChar(b, 0, adjustedLength, encoption);
    }

    public String IfxToJavaChar(byte[] b, int offset, int length, boolean encoption) throws IOException {
        return _IfxToJavaType.IfxToJavaChar(b, offset, length, "ISO8859_1", encoption);
    }

    public String IfxToJavaChar(byte[] b, String dbEnc, boolean encoption) throws IOException {
        return _IfxToJavaType.IfxToJavaChar(b, 0, b.length, dbEnc, encoption);
    }

}

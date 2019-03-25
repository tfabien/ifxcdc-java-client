/* 
 * @author dhudson -  
 * Created 28 Jul 2010 : 09:20:09 
 */

package com.conforama.informix.cdc;

public final class CDCConstants {

	// select * from syscdcrectypes

	public static final int CDC_REC_BEGINTX = 1;
	public static final int CDC_REC_COMMTX = 2;
	public static final int CDC_REC_RBTX = 3;

	public static final int CDC_REC_INSERT = 40;
	public static final int CDC_REC_DELETE = 41;

	public static final int CDC_REC_UPDBEF = 42;
	public static final int CDC_REC_UPDAFT = 43;

	public static final int CDC_REC_DISCARD = 62;
	public static final int CDC_REC_TRUNCATE = 119;

	public static final int CDC_REC_TABSCHEMA = 200;
	public static final int CDC_REC_TIMEOUT = 201;

	public static final int CDC_REC_ERROR = 202;

	public static final int NUM_CDCLOGRECTYPES = 12;

	public static final int CDC_HEADER_SIZE = 16;

	public static final int CDC_MIN_BUFFER_SIZE = 128;

	public static final int CDC_PKTSCHEME_LRECBINARY = 66;

}
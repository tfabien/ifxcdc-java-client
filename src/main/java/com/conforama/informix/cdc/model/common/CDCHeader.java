package com.conforama.informix.cdc.model.common;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.conforama.informix.cdc.CDCConstants;

public class CDCHeader {

	/**
	 * The number of bytes in the common and CDC record-specific headers.
	 */
	private final int headerSize;

	/**
	 * The number of bytes of data in the record after the common and CDC
	 * record-specific headers.
	 */
	private final int payloadSize;

	/**
	 * The packetization scheme number of one of the packetization schemes contained
	 * in the syscdcpacketschemes table. The only packetization scheme is 66,
	 * CDC_PKTSCHEME_LRECBINARY.
	 */
	private final int packetType;
	
	/**
	 * The record number of one of the CDC records contained in the syscdcrectypes
	 * table.
	 */
	private final int logRecordType;

	private final ByteBuffer header;
	private final ByteBuffer payload;

	/**
	 * Constructor.
	 * 
	 * @param data
	 * @param length
	 */
	public CDCHeader(byte[] data, int length) {

		ByteBuffer buffer = ByteBuffer.allocate(length);
		buffer.put(data, 0, length);
		buffer.flip();

		headerSize = buffer.getInt() - CDCConstants.CDC_HEADER_SIZE;
		payloadSize = buffer.getInt();

		// TODO: This must be 66
		packetType = buffer.getInt();

		// Log record type
		logRecordType = buffer.getInt();

		byte[] header = new byte[headerSize];
		byte[] payload = new byte[payloadSize];

		buffer.get(header);
		buffer.get(payload);

		this.header = ByteBuffer.wrap(header);
		this.payload = ByteBuffer.wrap(payload);
	}

	/**
	 * getHeaderSize
	 * 
	 * @return The number of bytes in the common and CDC record-specific headers.
	 */
	public int getHeaderSize() {
		return headerSize;
	}

	/**
	 * getPayloadSize
	 * 
	 * @return The number of bytes of data in the record after the common and CDC
	 *         record-specific headers.
	 */
	public int getPayloadSize() {
		return payloadSize;
	}

	/**
	 * getPacketType
	 * 
	 * @return The packetization scheme number of one of the packetization schemes
	 *         contained in the syscdcpacketschemes table. The only packetization
	 *         scheme is 66, CDC_PKTSCHEME_LRECBINARY.
	 */
	public int getPacketType() {
		return packetType;
	}

	/**
	 * theRecordNumber
	 * 
	 * @return The record number of one of the CDC records contained in the
	 *         syscdcrectypes table.
	 */
	public int getLogRecordType() {
		return logRecordType;
	}

	/**
	 * getPayload
	 * 
	 * @return
	 */
	public ByteBuffer getPayload() {
		return payload;
	}

	/**
	 * getHeader
	 * 
	 * @return
	 */
	public ByteBuffer getHeader() {
		return header;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

}
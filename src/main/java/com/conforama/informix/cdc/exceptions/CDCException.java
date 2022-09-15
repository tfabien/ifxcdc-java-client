package com.conforama.informix.cdc.exceptions;

public class CDCException extends RuntimeException {
    private CDCErrorCode cdcErrorCode;

    private long errorCode;

    public CDCException(CDCErrorCode cdcErrorCode) {
        super(cdcErrorCode.getMessage());
        this.cdcErrorCode = cdcErrorCode;
    }

    public CDCException(long errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
}

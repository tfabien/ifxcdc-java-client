package com.conforama.informix.cdc.exceptions;

public enum CDCErrorCodeImpl implements CDCErrorCode {
    BUFFER_UNDER_FLOW(1, "If there are fewer than length bytes remaining in this buffer"),
    CLASS_NOT_FOUND(2, "找不到类"),
    INNER_ERROR(900, "内部错误"),
    ;

    private final long code;
    private final String message;

    CDCErrorCodeImpl(long code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public long getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}

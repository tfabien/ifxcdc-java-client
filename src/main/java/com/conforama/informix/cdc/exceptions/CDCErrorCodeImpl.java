package com.conforama.informix.cdc.exceptions;

public enum CDCErrorCodeImpl implements CDCErrorCode {
    BUFFER_UNDER_FLOW(1, "If there are fewer than length bytes remaining in this buffer"),
    CLASS_NOT_FOUND(2, "找不到类"),
    NO_CONNECTION_DETAIL(3, "没有连接"),
    CANNOT_CONNECT(4, "无法建立连接"),
    UNABLE_TO_GET_CDC_CONNECTION(5, "无法获取CDC连接"),
    UNABLE_TO_OPEN_CDC_CONNECTION(6, "无法打开CDC连接"),
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

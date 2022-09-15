package com.conforama.informix.cdc.exceptions;

public interface CDCErrorCode {
    long getCode();

    String getMessage();
}

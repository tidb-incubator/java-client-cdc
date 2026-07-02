package org.tikv.cdc.exception;

public class CDCException extends RuntimeException {
    private final ErrorType errorCode;

    public CDCException(Throwable e, ErrorType errorCode) {
        super(e);
        this.errorCode = errorCode;
    }

    public CDCException(String msg, ErrorType errorCode) {
        super(msg);
        this.errorCode = errorCode;
    }

    public CDCException(String msg, Throwable e, ErrorType errorCode) {
        super(msg, e);
        this.errorCode = errorCode;
    }

    public ErrorType getErrorCode() {
        return errorCode;
    }
}

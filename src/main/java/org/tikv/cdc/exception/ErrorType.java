package org.tikv.cdc.exception;

/** error Type */
public enum ErrorType {
    ErrVersionIncompatible("CDC:ErrVersionIncompatible"),
    ErrClusterIDMismatch("CDC:ErrClusterIDMismatch"),
    ErrorEvent("CDC:ErrClusterIDMismatch");
    private final String message;

    ErrorType(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

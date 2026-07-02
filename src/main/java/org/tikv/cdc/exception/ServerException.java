package org.tikv.cdc.exception;

import java.io.IOException;

public class ServerException extends IOException {

    private int errorCode;
    private String sqlState;

    public ServerException(String message, int errorCode, String sqlState) {
        super(message);
        this.errorCode = errorCode;
        this.sqlState = sqlState;
    }

    /** @return MySQL-compatible server error code */
    public int getErrorCode() {
        return errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }
}

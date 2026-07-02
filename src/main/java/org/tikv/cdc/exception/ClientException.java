package org.tikv.cdc.exception;

import org.tikv.common.exception.TiKVException;

public class ClientException extends TiKVException {
    private static final long serialVersionUID = -6087861427311224215L;

    public ClientException(Throwable e) {
        super(e);
    }

    public ClientException(String msg) {
        super(msg);
    }

    public ClientException(String msg, Throwable e) {
        super(msg, e);
    }
}

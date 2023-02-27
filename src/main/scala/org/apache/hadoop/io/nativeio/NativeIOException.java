//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.util.Shell;

import java.io.IOException;

public class NativeIOException extends IOException {
    private static final long serialVersionUID = 1L;
    private Errno errno;
    private int errorCode;

    public NativeIOException(String msg, Errno errno) {
        super(msg);
        this.errno = errno;
        this.errorCode = 0;
    }

    public NativeIOException(String msg, int errorCode) {
        super(msg);
        this.errorCode = errorCode;
        this.errno = Errno.UNKNOWN;
    }

    public long getErrorCode() {
        return (long)this.errorCode;
    }

    public Errno getErrno() {
        return this.errno;
    }

    public String toString() {
        return Shell.WINDOWS ? this.errorCode + ": " + super.getMessage() : this.errno.toString() + ": " + super.getMessage();
    }
}

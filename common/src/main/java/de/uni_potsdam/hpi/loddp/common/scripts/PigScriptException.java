package de.uni_potsdam.hpi.loddp.common.scripts;

public class PigScriptException extends Exception {
    public PigScriptException(String message) {
        super(message);
    }

    public PigScriptException(String message, Throwable cause) {
        super(message, cause);
    }

    public PigScriptException(Throwable cause) {
        super(cause);
    }
}
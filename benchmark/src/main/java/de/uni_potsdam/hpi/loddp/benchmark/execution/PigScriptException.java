package de.uni_potsdam.hpi.loddp.benchmark.execution;

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
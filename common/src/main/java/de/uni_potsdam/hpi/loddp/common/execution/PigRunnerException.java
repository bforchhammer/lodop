package de.uni_potsdam.hpi.loddp.common.execution;

public class PigRunnerException extends Exception{
    public PigRunnerException(String message) {
        super(message);
    }

    public PigRunnerException(String message, Throwable cause) {
        super(message, cause);
    }

    public PigRunnerException(Throwable cause) {
        super(cause);
    }
}

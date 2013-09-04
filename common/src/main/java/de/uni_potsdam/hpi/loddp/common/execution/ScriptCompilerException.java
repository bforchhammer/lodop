package de.uni_potsdam.hpi.loddp.common.execution;

import java.io.IOException;

public class ScriptCompilerException extends Exception {
    public ScriptCompilerException(String message) {
        super(message);
    }

    public ScriptCompilerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ScriptCompilerException(Throwable cause) {
        super(cause);
    }
}

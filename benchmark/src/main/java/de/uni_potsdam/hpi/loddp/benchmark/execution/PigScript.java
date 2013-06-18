package de.uni_potsdam.hpi.loddp.benchmark.execution;

import java.io.InputStream;

/**
 * A pig script.
 */
public abstract class PigScript {

    /**
     * Returns a new InputStream for wrapped pig script file.
     *
     * @return InputStream instance.
     */
    public abstract InputStream getNewInputStream();

    /**
     * Returns the filename of the pig script (without the extension).
     *
     * @return the name of the pig script.
     */
    public abstract String getScriptName();

    @Override
    public String toString() {
        return String.format("PigScript[%s]", getScriptName());
    }

}

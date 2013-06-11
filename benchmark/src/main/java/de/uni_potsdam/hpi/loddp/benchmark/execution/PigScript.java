package de.uni_potsdam.hpi.loddp.benchmark.execution;

import java.io.InputStream;

/**
 * A pig script.
 */
public abstract class PigScript {
    private String resultAlias;

    /**
     * Constructor.
     *
     * @param resultAlias
     */
    public PigScript(String resultAlias) {
        this.resultAlias = resultAlias;
    }

    /**
     * Get the result alias for this pig script.
     *
     * @return The result alias.
     */
    public String getResultAlias() {
        return resultAlias;
    }

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

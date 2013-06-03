package de.uni_potsdam.hpi.loddp.benchmark;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * A pig script.
 */
public class PigScript {
    private File script;
    private String resultAlias;
    private InputStream inputStream;

    /**
     * Constructor.
     *
     * @param script
     * @param resultAlias
     */
    public PigScript(File script, String resultAlias) {
        this.script = script;
        this.resultAlias = resultAlias;
    }

    /**
     * Creates a PigScript object for the given file.
     *
     * Tries to automatically determine the alias of the result statement with {@link
     * PigScriptHelper#guessResultAlias}.
     *
     * @param file
     *
     * @return Instance of PigScript, or null in case of error.
     *
     * @throws PigScriptException
     */
    public static PigScript fromFile(File file) throws PigScriptException {
        try {
            String alias = PigScriptHelper.guessResultAlias(file);
            return new PigScript(file, alias);
        } catch (IOException e) {
            throw new PigScriptException("Cannot create PigScript from file", e);
        }
    }

    /**
     * Creates a list of PigScript objects for the given set of files.
     *
     * Tries to automatically determine the result alias for each script file.
     *
     * @param files
     *
     * @return Set of PigScript instances.
     *
     * @see #fromFile
     */
    public static Set<PigScript> fromFiles(File[] files) {
        Set<PigScript> scripts = new HashSet<PigScript>(files.length);
        for (File file : files) {
            try {
                PigScript script = fromFile(file);
                scripts.add(script);
            } catch (PigScriptException e) {
                PigScriptHelper.log.error(e.getMessage(), e.getCause());
            }
        }
        return scripts;
    }

    public String getResultAlias() {
        return resultAlias;
    }

    /**
     * Get InputStream for wrapped pig script file.
     *
     * @return InputStream instance.
     */
    public InputStream getInputStream() {
        if (inputStream == null) {
            try {
                inputStream = new FileInputStream(script);
            } catch (FileNotFoundException e) {
                // Should not happen because usually we open and read the file already during initialization, see #fromFile
                PigScriptHelper.log.error(e.getMessage(), e);
            }
        }
        return inputStream;
    }

    /**
     * Returns the filename of the pig script (without the extension).
     *
     * @return the name of the pig script.
     */
    public String getScriptName() {
        return org.apache.commons.io.FilenameUtils.removeExtension(script.getName());
    }

    @Override
    public String toString() {
        return String.format("PigScript[%s]", getScriptName());
    }

}

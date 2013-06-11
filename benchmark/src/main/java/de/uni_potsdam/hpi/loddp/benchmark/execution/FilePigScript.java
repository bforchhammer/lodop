package de.uni_potsdam.hpi.loddp.benchmark.execution;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * A pig script residing on the local filesystem.
 */
public class FilePigScript extends PigScript {
    private File script;

    /**
     * Constructor.
     *
     * @param resultAlias
     * @param script
     */
    public FilePigScript(String resultAlias, File script) {
        super(resultAlias);
        this.script = script;
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
            return new FilePigScript(alias, file);
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
                PigScript script = FilePigScript.fromFile(file);
                scripts.add(script);
            } catch (PigScriptException e) {
                PigScriptHelper.log.error(e.getMessage(), e.getCause());
            }
        }
        return scripts;
    }

    @Override
    public InputStream getNewInputStream() {
        try {
            return new FileInputStream(script);
        } catch (FileNotFoundException e) {
            // Should not happen because usually we open and read the file already during initialization, see #fromFile
            PigScriptHelper.log.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public String getScriptName() {
        return org.apache.commons.io.FilenameUtils.removeExtension(script.getName());
    }
}

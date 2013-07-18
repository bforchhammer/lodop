package de.uni_potsdam.hpi.loddp.common.scripts;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * A pig script residing on the local filesystem.
 */
public class FilePigScript extends PigScript {
    private File script;

    /**
     * Constructor.
     *
     * @param script
     */
    public FilePigScript(File script) {
        this.script = script;
    }

    @Override
    public InputStream getNewInputStream() {
        try {
            return new FileInputStream(script);
        } catch (FileNotFoundException e) {
            // Should not happen because usually we open and read the file already during initialization,
            // see PigScriptFactory#fromFile.
            PigScriptFactory.log.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public String getScriptName() {
        return FilenameUtils.removeExtension(script.getName());
    }
}

package de.uni_potsdam.hpi.loddp.common.scripts;

import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.util.jar.JarEntry;

/**
 * A pig script residing in a jar file.
 */
public class JarPigScript extends PigScript {

    private final JarURLConnection jarConnection;
    private final JarEntry entry;

    protected JarPigScript(JarURLConnection jarConnection, JarEntry entry) {
        this.jarConnection = jarConnection;
        this.entry = entry;
    }

    @Override
    public InputStream getNewInputStream() {
        try {
            return jarConnection.getJarFile().getInputStream(entry);
        } catch (IOException e) {
            // Should not happen because usually we open and read the file already during initialization, see #fromJarConnection
            PigScriptFactory.log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public String getScriptName() {
        return FilenameUtils.getBaseName(entry.getName());
    }
}


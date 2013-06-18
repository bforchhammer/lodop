package de.uni_potsdam.hpi.loddp.benchmark.execution;

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

    public static JarPigScript fromJarConnection(JarURLConnection jarConnection, JarEntry entry) throws PigScriptException {
        try {
            // Check that jar file can be read by opening an input stream:
            InputStream is = jarConnection.getJarFile().getInputStream(entry);
            return new JarPigScript(jarConnection, entry);
        } catch (IOException e) {
            throw new PigScriptException("Cannot create PigScript from jar connection.", e);
        }
    }

    @Override
    public InputStream getNewInputStream() {
        try {
            return jarConnection.getJarFile().getInputStream(entry);
        } catch (IOException e) {
            // Should not happen because usually we open and read the file already during initialization, see #fromJarConnection
            PigScriptHelper.log.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public String getScriptName() {
        // @todo remove folder name.
        return org.apache.commons.io.FilenameUtils.removeExtension(entry.getName());
    }
}


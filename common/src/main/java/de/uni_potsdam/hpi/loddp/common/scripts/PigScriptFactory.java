package de.uni_potsdam.hpi.loddp.common.scripts;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.jar.JarEntry;

/**
 * Various helper functions for loading and parsing pig scripts.
 */
public class PigScriptFactory {
    /**
     * Name of directory containing pig scripts. Used for look-up on classpath in {@link #findPigScripts}.
     */
    protected static String PIG_SCRIPTS_DIRECTORY = "pig-queries";
    protected static final Log log = LogFactory.getLog(PigScriptFactory.class);
    /**
     * File filter which only accepts files having the "pig" file extension.
     */
    private static final FileFilter PIG_EXTENSION_FILTER = new FileFilter() {
        public boolean accept(File file) {
            return file.isFile() && FilenameUtils.isExtension(file.getName(), "pig");
        }
    };

    public static void setPigScriptsDirectory(String PIG_SCRIPTS_DIRECTORY) {
        PigScriptFactory.PIG_SCRIPTS_DIRECTORY = PIG_SCRIPTS_DIRECTORY;
    }

    /**
     * Loads and returns a set of PigScript objects matching the given list of script names.
     */
    public static Set<PigScript> findPigScripts(String[] whitelist) {
        return findPigScripts(whitelist, false);
    }

    /**
     * Loads and returns a set of PigScript objects matching the given list of script names. If inverse is set to true,
     * returns all scripts which are NOT matching the given list of script names.
     */
    public static Set<PigScript> findPigScripts(String[] whitelist, boolean inverse) {
        Set<PigScript> scripts = loadPigScripts();
        Set<PigScript> scripts_new = new HashSet<PigScript>();
        String regex = stringToPattern(whitelist);
        for (PigScript s : scripts) {
            if (s.getScriptName().matches(regex) != inverse) {
                scripts_new.add(s);
            }
        }

        StringBuilder sb = new StringBuilder();
        Iterator<PigScript> it = scripts_new.iterator();
        while (it.hasNext()) {
            if (scripts_new.size() < 2) sb.append(" ");
            else sb.append("\n- ");
            sb.append(it.next().getScriptName());
        }
        log.info(String.format("Using %d pig script(s): %s", scripts_new.size(), sb.toString()));

        return scripts_new;
    }

    /**
     * Converts the given array of strings into a regular expression pattern to match against. Also supports asterisks
     * (*) as a wildcard.
     */
    private static String stringToPattern(String[] whitelist) {
        StringBuilder pattern = new StringBuilder();
        for (String s : whitelist) {
            if (pattern.length() > 0) pattern.append("|");
            pattern.append(s.replace("*", ".*"));
        }
        return pattern.toString();
    }

    /**
     * Loads and returns a set of PigScript objects for all scripts found in the resources folder.
     */
    public static Set<PigScript> findPigScripts() {
        Set<PigScript> scripts = loadPigScripts();
        log.info(String.format("Using ALL pig scripts."));
        return scripts;
    }

    /**
     * Loads pig scripts from the resources folder.
     *
     * The method looks for folders named {@link #PIG_SCRIPTS_DIRECTORY} on the class path, and then loads all files
     * contained within these folders and ending in <code>*.pig</code>.
     */
    private static Set<PigScript> loadPigScripts() {
        Set<PigScript> scripts = new HashSet<PigScript>();
        try {
            Enumeration<URL> urls = ClassLoader.getSystemClassLoader().getResources(PIG_SCRIPTS_DIRECTORY);

            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();

                // Load pig scripts from a JAR File (Hm, is there a better way to do this..?)
                URLConnection urlConnection = url.openConnection();
                if (urlConnection instanceof JarURLConnection) {
                    loadPigScripts((JarURLConnection) urlConnection, scripts);
                }

                // Try loading pig scripts via File object from local FS path.
                else {
                    loadPigScripts(url, scripts);
                }
            }
        } catch (IOException e) {
            // Something wrong with the class loader
            log.error("Failed to load pig scripts", e);
        }
        log.debug(String.format("Found %d pig scripts.", scripts.size()));
        return scripts;
    }

    /**
     * Load pig scripts from a JAR File.
     *
     * @param jarConnection
     * @param scripts
     */
    private static void loadPigScripts(JarURLConnection jarConnection, Set<PigScript> scripts) {
        Enumeration<JarEntry> entries;
        try {
            entries = jarConnection.getJarFile().entries();
        } catch (IOException e) {
            log.error("Cannot get entries from jar file.", e);
            return;
        }

        Set<PigScript> s = new HashSet<PigScript>();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            if (!entry.isDirectory() && entry.getName().startsWith(PIG_SCRIPTS_DIRECTORY)) {
                try {
                    PigScript script = fromJarConnection(jarConnection, entry);
                    s.add(script);
                } catch (Exception e) {
                    log.error("Cannot create pig script for: " + entry.getName(), e);
                }
            }
        }
        log.info(String.format("Found %d pig script(s) in: %s", s.size(), jarConnection.getURL()));
        scripts.addAll(s);
    }

    /**
     * Creates a new PigScript instance.
     *
     * @param jarConnection
     * @param entry
     *
     * @return
     *
     * @throws PigScriptException
     */
    public static PigScript fromJarConnection(JarURLConnection jarConnection, JarEntry entry) throws PigScriptException {
        try {
            // Check that jar file can be read by opening an input stream:
            jarConnection.getJarFile().getInputStream(entry).available();
            return new JarPigScript(jarConnection, entry);
        } catch (IOException e) {
            throw new PigScriptException("Cannot create PigScript from jar connection.", e);
        }
    }

    /**
     * Try loading pig scripts via File object from local FS path.
     *
     * @param fileURL
     * @param scripts
     */
    private static void loadPigScripts(URL fileURL, Set<PigScript> scripts) {
        File file;
        try {
            file = new File(fileURL.toURI());
        } catch (URISyntaxException e) {
            log.error("Failed to load pig scripts from " + fileURL, e);
            return;
        } catch (IllegalArgumentException e) {
            log.error("Failed to load pig scripts from " + fileURL, e);
            return;
        }

        if (!file.isDirectory()) {
            return;
        }

        File[] files = file.listFiles(PIG_EXTENSION_FILTER);
        Set<PigScript> s = fromFiles(files);
        log.info(String.format("Found %d pig script(s) in: %s", s.size(), file));
        scripts.addAll(s);
    }

    /**
     * Creates a PigScript object for the given file.
     *
     * @param file
     *
     * @return Instance of PigScript, or null in case of error.
     *
     * @throws PigScriptException
     */
    public static PigScript fromFile(File file) throws PigScriptException {
        try {
            // Check that we can create a valid input stream.
            new FileInputStream(file).available();
            return new FilePigScript(file);
        } catch (IOException e) {
            throw new PigScriptException("Cannot create PigScript from file", e);
        }
    }

    /**
     * Creates a list of PigScript objects for the given set of files.
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
                log.error(e.getMessage(), e.getCause());
            }
        }
        return scripts;
    }
}

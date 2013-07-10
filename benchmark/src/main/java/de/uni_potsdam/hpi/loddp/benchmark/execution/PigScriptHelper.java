package de.uni_potsdam.hpi.loddp.benchmark.execution;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileFilter;
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
public class PigScriptHelper {
    /**
     * File filter which only accepts files having the "pig" file extension.
     */
    public static final FileFilter PIG_EXTENSION_FILTER = new FileFilter() {
        public boolean accept(File file) {
            return file.isFile() && FilenameUtils.isExtension(file.getName(), "pig");
        }
    };
    /**
     * Name of directory containing pig scripts. Used for look-up on classpath in {@link #findPigScripts}.
     */
    public static final String PIG_SCRIPTS_DIRECTORY = "pig-queries";
    protected static final Log log = LogFactory.getLog(PigScriptHelper.class);

    /**
     * Loads and returns a set of PigScript objects matching the given list of script names.
     */
    public static Set<PigScript> findPigScripts(String[] whitelist) {
        Set<PigScript> scripts = loadPigScripts();
        Set<PigScript> scripts_new = new HashSet<PigScript>();
        String regex = stringToPattern(whitelist);
        for (PigScript s : scripts) {
            if (s.getScriptName().matches(regex)) {
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
        log.debug(String.format("Executing %d pig script(s): %s", scripts_new.size(), sb.toString()));

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
        log.debug(String.format("Executing ALL pig scripts."));
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
                    log.debug("Looking at: " + url);
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

    private static void loadPigScripts(JarURLConnection jarConnection, Set<PigScript> scripts) {
        Enumeration<JarEntry> entries;
        try {
            entries = jarConnection.getJarFile().entries();
        } catch (IOException e) {
            log.error("Cannot get entries from jar file.", e);
            return;
        }

        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            if (!entry.isDirectory() && entry.getName().startsWith(PIG_SCRIPTS_DIRECTORY)) {
                log.info("Found pig script: " + entry.getName());
                try {
                    PigScript script = JarPigScript.fromJarConnection(jarConnection, entry);
                    scripts.add(script);
                } catch (Exception e) {
                    log.error("Cannot create pig script for: " + entry.getName(), e);
                }
            }
        }
    }

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
        Set<PigScript> s = FilePigScript.fromFiles(files);
        log.info(String.format("Found %d pig script(s) in: %s", s.size(), file));
        scripts.addAll(s);
    }

    /**
     * Blacklist for executing only scripts matching any of the given set of script names.
     *
     * @param scriptNames
     *
     * @return
     */
    public static Set<String> getBlackList(String[] scriptNames) {
        Set<String> blacklist = getBlackList(SCRIPT_LIST.NONE);
        for (String s : scriptNames)
            blacklist.remove(s);
        return blacklist;
    }

    /**
     * Blacklist for executing only scripts matching the given script name.
     *
     * @param scriptName
     *
     * @return
     */
    public static Set<String> getBlackList(String scriptName) {
        Set<String> blacklist = getBlackList(SCRIPT_LIST.NONE);
        blacklist.remove(scriptName);
        return blacklist;
    }

    /**
     * Blacklist for executing certain groups of scripts, depending on type.
     *
     * @param type
     *
     * @return
     */
    public static Set<String> getBlackList(SCRIPT_LIST type) {
        Set<String> blacklist = new HashSet<String>();
        switch (type) {
            case NO_COOC:
                blacklist.add("incoming_property_cooc");
                blacklist.add("property_cooc_by_entities");
                blacklist.add("property_cooc_by_urls");
                break;
            case ONLY_COOC:
                blacklist.add("classes_by_entity");
                blacklist.add("classes_by_url");
                blacklist.add("classes_by_tld");
                blacklist.add("classes_by_pld");
                //blacklist.add("incoming_property_cooc");
                blacklist.add("number_of_triples");
                blacklist.add("number_of_instances");
                //blacklist.add("property_cooc_by_entities");
                //blacklist.add("property_cooc_by_urls");
                blacklist.add("properties_by_entity");
                blacklist.add("properties_by_pld");
                blacklist.add("properties_by_statement");
                blacklist.add("properties_by_tld");
                blacklist.add("properties_by_url");
                blacklist.add("vocabularies_by_entity");
                blacklist.add("vocabularies_by_pld");
                blacklist.add("vocabularies_by_tld");
                blacklist.add("vocabularies_by_url");
                break;
            case NONE:
                blacklist.add("classes_by_entity");
                blacklist.add("classes_by_url");
                blacklist.add("classes_by_tld");
                blacklist.add("classes_by_pld");
                blacklist.add("incoming_property_cooc");
                blacklist.add("number_of_triples");
                blacklist.add("number_of_instances");
                blacklist.add("property_cooc_by_entities");
                blacklist.add("property_cooc_by_urls");
                blacklist.add("properties_by_entity");
                blacklist.add("properties_by_pld");
                blacklist.add("properties_by_statement");
                blacklist.add("properties_by_tld");
                blacklist.add("properties_by_url");
                blacklist.add("vocabularies_by_entity");
                blacklist.add("vocabularies_by_pld");
                blacklist.add("vocabularies_by_tld");
                blacklist.add("vocabularies_by_url");
                break;
        }
        return blacklist;
    }

    public static enum SCRIPT_LIST {
        ALL, NONE, ONLY_COOC, NO_COOC
    }

}

package de.uni_potsdam.hpi.loddp.benchmark.execution;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.HashSet;
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
     * Loads pig scripts from the resources folder.
     *
     * The method looks for folders named {@link #PIG_SCRIPTS_DIRECTORY} on the class path, and then loads all files
     * contained within these folders and ending in <code>*.pig</code>.
     */
    public static Set<PigScript> findPigScripts() {
        Set<PigScript> scripts = new HashSet<PigScript>();
        try {
            Enumeration<URL> urls = ClassLoader.getSystemClassLoader().getResources(PIG_SCRIPTS_DIRECTORY);

            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();

                // Load pig scripts from a JAR File (Hm, is there a better way to do this..?)
                URLConnection urlConnection = url.openConnection();
                if (urlConnection instanceof JarURLConnection) {
                    log.debug("Looking at: " + url);
                    findPigScripts((JarURLConnection) urlConnection, scripts);
                }

                // Try loading pig scripts via File object from local FS path.
                else {
                    findPigScripts(url, scripts);
                }
            }
        } catch (IOException e) {
            // Something wrong with the class loader
            log.error("Failed to load pig scripts", e);
        }
        log.info(String.format("Loaded %d pig scripts.", scripts.size()));
        return scripts;
    }

    private static void findPigScripts(JarURLConnection jarConnection, Set<PigScript> scripts) {
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

    private static void findPigScripts(URL fileURL, Set<PigScript> scripts) {
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
        log.info("Found pig scripts in: " + file);
        scripts.addAll(FilePigScript.fromFiles(files));
    }

    /**
     * Returns last line of given file.
     *
     * @param file
     *
     * @return
     *
     * @throws IOException
     */
    private static String tail(File file) throws IOException {
        RandomAccessFile fileHandler = null;
        try {
            fileHandler = new RandomAccessFile(file, "r");
            long fileLength = file.length() - 1;
            StringBuilder sb = new StringBuilder();

            for (long filePointer = fileLength; filePointer != -1; filePointer--) {
                fileHandler.seek(filePointer);
                int readByte = fileHandler.readByte();

                if (readByte == 0xA) {
                    if (filePointer == fileLength) {
                        continue;
                    } else {
                        break;
                    }
                } else if (readByte == 0xD) {
                    if (filePointer == fileLength - 1) {
                        continue;
                    } else {
                        break;
                    }
                }

                sb.append((char) readByte);
            }

            String lastLine = sb.reverse().toString();
            return lastLine;
        } finally {
            if (fileHandler != null) fileHandler.close();
        }

    }

    /**
     * Reads the last non-empty line from the given file.
     *
     * @param file
     *
     * @return
     *
     * @throws IOException
     */
    private static String readLastLine(File file) throws IOException {
        String line = null;
        do {
            line = tail(file);
        }
        while (line.isEmpty());
        return line;
    }

    private static String readLastLine(InputStream is) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while (in.ready()) {
            String line_tmp = in.readLine();
            // ignore empty lines
            if (!line_tmp.isEmpty()) line = line_tmp;
        }
        return line;
    }

    /**
     * Tries to determine the result alias for a given pig script.
     *
     * Assumes that the alias for the expected result resides on the last non-empty line of the script. Also assumes,
     * that the last line contains a pig-latin statement, which assigns something to an alias (e.g.
     * <code>topClassesByEntities = ORDER classCounts BY cnt DESC;</code>.
     *
     * Therefore, the alias is determined by parsing the last line of the given file, and returning the left-hand side
     * of the pig-latin assignment. If the last line cannot be determined, or it does not contain an assignment, the
     * method throws an Exception.
     *
     * @param file A pig script.
     *
     * @return Result alias for given pig script.
     *
     * @throws IOException
     */
    public static String guessResultAlias(File file) throws IOException {
        String lastStatement = readLastLine(file);
        return guessResultAlias(lastStatement);
    }

    /**
     * Tries to determine the result alias for a given pig script.
     *
     * Assumes that the alias for the expected result resides on the last non-empty line of the script. Also assumes,
     * that the last line contains a pig-latin statement, which assigns something to an alias (e.g.
     * <code>topClassesByEntities = ORDER classCounts BY cnt DESC;</code>.
     *
     * Therefore, the alias is determined by parsing the last line of the given file, and returning the left-hand side
     * of the pig-latin assignment. If the last line cannot be determined, or it does not contain an assignment, the
     * method throws an Exception.
     *
     * @param is An input stream to read the script from.
     *
     * @return Result alias for given pig script.
     *
     * @throws IOException
     */
    public static String guessResultAlias(InputStream is) throws IOException {
        String lastStatement = readLastLine(is);
        return guessResultAlias(lastStatement);
    }

    /**
     * Tries to determine the result alias for the given pig latin statement.
     *
     * Assumes that the given string represent a pig latin statement which assigns something to an alias, (e.g.
     * <code>topClassesByEntities = ORDER classCounts BY cnt DESC;</code>. This method returns the left-hand side of the
     * given pig latin assignment.
     *
     * If the statement does not contain an assignment, the method throws an Exception.
     *
     * @param lastStatement A pig latin statement.
     *
     * @return Result alias for given pig script.
     *
     * @throws IOException
     */
    private static String guessResultAlias(String lastStatement) throws IOException {
        String[] pieces = lastStatement.split("=");
        if (pieces.length < 2) {
            throw new IOException(String.format("Statement is not a valid assignment: %s", lastStatement));
        }
        return pieces[0].trim();
    }


}

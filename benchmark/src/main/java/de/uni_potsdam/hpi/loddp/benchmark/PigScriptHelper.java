package de.uni_potsdam.hpi.loddp.benchmark;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

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
                File file = new File(urls.nextElement().toURI());
                if (!file.isDirectory()) continue;
                File[] files = file.listFiles(PIG_EXTENSION_FILTER);
                log.info("Found pig scripts in: " + file);
                scripts.addAll(PigScript.fromFiles(files));
            }
        } catch (IOException e) {
            // Something wrong with the class loader
            log.error("Failed to load pig scripts", e);
        } catch (URISyntaxException e) {
            // Error in URI-to-File conversion
            log.error("Failed to load pig scripts", e);
        }
        log.info(String.format("Loaded %d pig scripts.", scripts.size()));
        return scripts;
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
        String[] pieces = lastStatement.split("=");
        if (pieces.length < 2) {
            throw new IOException(String.format("Last line of %s is not a valid assignment: %s", file.getName(),
                lastStatement));
        }
        String alias = pieces[0].trim();
        return alias;
    }
}

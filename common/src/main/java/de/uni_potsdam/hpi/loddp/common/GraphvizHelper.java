package de.uni_potsdam.hpi.loddp.common;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;

public class GraphvizHelper {

    private static final boolean DEFAULT_DELETE_DOT_FILE = true;

    public static void convertToImage(String imageType, File dotFile) throws IOException {
        convertToImage(imageType, dotFile, DEFAULT_DELETE_DOT_FILE);
    }

    public static void convertToImage(String imageType, File dotFile, boolean deleteDotFile) throws IOException {
        String imageFilename = FilenameUtils.removeExtension(dotFile.getAbsolutePath());
        File imageFile = new File(imageFilename + '.' + imageType);
        convertToImage(imageType, dotFile, imageFile, deleteDotFile);
    }

    public static void convertToImage(String imageType, File dotFile, File imageFile) throws IOException {
        convertToImage(imageType, dotFile, imageFile, DEFAULT_DELETE_DOT_FILE);
    }

    public static void convertToImage(String imageType, File dotFile, File imageFile, boolean deleteDotFile) throws IOException {
        try {
            Runtime rt = Runtime.getRuntime();
            String[] args = {"/usr/bin/dot",
                "-T" + imageType,
                dotFile.getAbsolutePath(),
                "-o", imageFile.getAbsolutePath()
            };
            Process p = rt.exec(args);
            p.waitFor();
            if (deleteDotFile) {
                dotFile.delete();
            }
        } catch (InterruptedException e) {
            throw new IOException("Cannot output job graph as png graph.", e);
        } catch (IOException e) {
            throw new IOException("Cannot output job graph as png graph.", e);
        }
    }
}

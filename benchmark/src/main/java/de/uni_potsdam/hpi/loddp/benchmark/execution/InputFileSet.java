package de.uni_potsdam.hpi.loddp.benchmark.execution;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a set of input files.
 */
public class InputFileSet {
    private final String identifier;
    private final Map<Long, InputFile> filesBySize;

    private InputFileSet(String identifier) {
        this.identifier = identifier;
        filesBySize = new TreeMap<Long, InputFile>();
    }

    public static InputFileSet createLogarithmic(String identifier, String prefix, int start, int end) {
        return createLogarithmic(identifier, prefix, start, end, 10);
    }

    public static InputFileSet createLogarithmic(String identifier, String prefix, int start, int end, int factor) {
        return createLogarithmic(identifier, prefix, start, end, factor, ".nq.gz");
    }

    public static InputFileSet createLogarithmic(String identifier, String prefix, int start, int end, int factor,
                                                 String suffix) {
        InputFileSet fileSet = new InputFileSet(identifier);
        for (int i = start; i <= end; i *= factor) {
            String filename = prefix + i + suffix;
            fileSet.addFile((long) i, new InputFile(filename, i, fileSet));
        }
        return fileSet;
    }

    public static InputFileSet createLinear(String identifier, String prefix, int start, int end, String suffix, int sizeFactor) {
        return createLinear(identifier, prefix, start, end, 1, suffix, sizeFactor);
    }

    public static InputFileSet createLinear(String identifier, String prefix, int start, int end, int addend,
                                            String suffix, int sizeFactor) {
        InputFileSet fileSet = new InputFileSet(identifier);
        for (int i = start; i <= end; i += addend) {
            String filename = prefix + i + suffix;
            long inputSize = i * sizeFactor;
            fileSet.addFile(inputSize, new InputFile(filename, inputSize, fileSet));
        }
        return fileSet;
    }

    private void addFile(long inputSize, InputFile file) {
        filesBySize.put(inputSize, file);
    }

    public String getIdentifier() {
        return identifier;
    }

    public Collection<InputFile> getAll() {
        return filesBySize.values();
    }

    public InputFile getBySize(long size) {
        return filesBySize.get(size);
    }
}

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

    public InputFileSet(String identifier, String prefix, int start, int end) {
        this(identifier, prefix, start, end, 10);
    }

    public InputFileSet(String identifier, String prefix, int start, int end, int factor) {
        this(identifier, prefix, start, end, factor, ".nq.gz");
    }

    public InputFileSet(String identifier, String prefix, int start, int end, int factor, String suffix) {
        this.identifier = identifier;
        filesBySize = new TreeMap<Long, InputFile>();
        for (int i = start; i <= end; i *= factor) {
            String filename = prefix + i + suffix;
            filesBySize.put((long) i, new InputFile(filename, i, this));
        }
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

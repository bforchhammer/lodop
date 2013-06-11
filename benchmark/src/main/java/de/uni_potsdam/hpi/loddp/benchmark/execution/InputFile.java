package de.uni_potsdam.hpi.loddp.benchmark.execution;

/**
 * Represents an input file.
 */
public class InputFile {
    private String filename;
    private long tupleCount;
    private InputFileSet fileSet;

    /**
     * Constructor.
     *
     * @param filename
     * @param tupleCount
     * @param fileSet
     */
    public InputFile(String filename, long tupleCount, InputFileSet fileSet) {
        this.filename = filename;
        this.tupleCount = tupleCount;
        this.fileSet = fileSet;
    }

    /**
     * Constructor.
     *
     * @param filename
     * @param tupleCount
     */
    public InputFile(String filename, long tupleCount) {
        this(filename, tupleCount, null);
    }

    /**
     * Constructor.
     *
     * @param filename
     */
    public InputFile(String filename) {
        this(filename, 0);
    }

    public long getTupleCount() {
        return tupleCount;
    }

    public String getFilename() {
        return filename;
    }

    public String getFileSetIdentifier() {
        return fileSet == null ? "N/A" : fileSet.getIdentifier();
    }
}

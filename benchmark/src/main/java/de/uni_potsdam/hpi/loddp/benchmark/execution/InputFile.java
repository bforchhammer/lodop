package de.uni_potsdam.hpi.loddp.benchmark.execution;

/**
 * Represents an input file.
 */
public class InputFile {
    private String filename;
    private long tupleCount;
    private InputFileSet fileSet;
    private String fileSetIdentifier = null;

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
        if (fileSet != null) {
            this.fileSetIdentifier = fileSet.getIdentifier();
        } else {
            this.fileSetIdentifier = InputFileHelper.guessFilesetIdentifier(filename);
        }
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
        this(filename, InputFileHelper.guessTupleCount(filename));
    }

    public long getTupleCount() {
        return tupleCount;
    }

    public String getFilename() {
        return filename;
    }

    public String getFileSetIdentifier() {
        return fileSetIdentifier;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getFileSetIdentifier())
            .append(" / ").append(getTupleCount())
            .append(" \t(").append(getFilename()).append(")");
        return sb.toString();
    }
}

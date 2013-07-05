package de.uni_potsdam.hpi.loddp.benchmark.execution;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

public class ScriptRunner {

    protected static final Log log = LogFactory.getLog(ScriptRunner.class);
    private final String hdfsWorkingDirectory;
    private final Properties serverProperties;
    private final boolean reuseServer;
    private PigServer pig;

    /**
     * Constructor.
     *
     * @param location The hadoop server location.
     */
    public ScriptRunner(HADOOP_LOCATION location, String hdfsWorkingDirectory) {
        this(location, false, hdfsWorkingDirectory);
    }

    /**
     * Constructor.
     *
     * @param reuseServer Whether to reuse the pig server, or create a new one for each invocation of {@link
     *                    #runScript}. If the pig server is reused, then calculations from previous scripts can be
     *                    reused as long as they have the same alias. This can also lead to errors, if two aliases in
     *                    different scripts represent different data sets.
     *
     * @throws IOException
     */
    public ScriptRunner(HADOOP_LOCATION location, boolean reuseServer, String hdfsWorkingDirectory) {
        this.reuseServer = reuseServer;
        this.serverProperties = generateProperties(location);

        // Make sure hdfs directory ends with a slash.
        if (hdfsWorkingDirectory.isEmpty()) hdfsWorkingDirectory = "./";
        else if (!hdfsWorkingDirectory.endsWith("/")) hdfsWorkingDirectory += '/';
        this.hdfsWorkingDirectory = hdfsWorkingDirectory;

        log.info(String.format("ScriptRunner is using %s PigServer(s) for executing scripts.",
            reuseServer ? "ONE" : "SEPARATE"));
    }

    /**
     * Creates a properties instance with hadoop cluster configuration settings depending on the given Hadoop location.
     *
     * @param type The server address (cluster or local).
     *
     * @return A properties object with at least the following two properties filled in: "fs.default.name",
     *         "mapred.job.tracker".
     */
    private static Properties generateProperties(HADOOP_LOCATION type) {
        Properties properties = new Properties();
        switch (type) {
            case HPI_CLUSTER:
                properties.setProperty("fs.default.name", "hdfs://tenemhead2.hpi.uni-potsdam.de");
                properties.setProperty("mapred.job.tracker", "tenemhead2.hpi.uni-potsdam.de:9001");
                break;
            case LOCALHOST:
                properties.setProperty("fs.default.name", "hdfs://localhost:9000");
                properties.setProperty("mapred.job.tracker", "localhost:9001");
                break;
        }
        return properties;
    }

    /**
     * Initialises the {@link #pig} field.
     *
     * @throws IOException
     */
    private void initialisePig() throws IOException {
        this.pig = new PigServer(ExecType.MAPREDUCE, serverProperties);

        // Register UDF + required libraries.
        this.pig.registerJar("ldif-single-0.5.1-jar-with-dependencies.jar");
        this.pig.registerJar("loddesc-core-0.1.jar");
        this.pig.registerJar("piggybank.jar");
        this.pig.registerJar("loddp-udf.jar");

        this.log.debug("Created new pig server.");
    }

    /**
     * Load quads from the specified filename (must be on HDFS) into the 'quads' alias, using the wbsg QuadLoader.
     *
     * @param inputFile The input file.
     *
     * @throws IOException
     */
    private void loadQuads(InputFile inputFile) throws IOException {
        String filename = hdfsWorkingDirectory + inputFile.getFilename();
        if (!getPig().existsFile(filename)) {
            throw new IOException("File not found on HDFS: " + filename);
        }
        String statement = "quads = LOAD '" + filename + "' USING de.wbsg.loddesc.importer.QuadLoader() AS " +
            "(subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), " +
            "graph:chararray);";
        getPig().registerQuery(statement);
        log.info("Loading quads from " + filename);
    }

    /**
     * Get the PigServer instance. Initialises one, if it doesn't exist.
     *
     * @return The PigServer instance.
     *
     * @throws IOException Thrown if the PigServer cannot be initialised properly.
     */
    protected PigServer getPig() throws IOException {
        if (pig == null) {
            initialisePig();
        }
        return pig;
    }

    /**
     * Execute the given pig script.
     *
     * Overrides any already existing results.
     *
     * @param script    A pig script.
     * @param inputFile The input file.
     */
    public PigStats runScript(PigScript script, InputFile inputFile) {
        return runScript(script, inputFile, true);
    }

    /**
     * Execute the given pig script.
     *
     * @param script          A pig script.
     * @param inputFile       The input file.
     * @param replaceExisting Whether to override existing results.
     */
    public PigStats runScript(PigScript script, InputFile inputFile, boolean replaceExisting) {
        // If server reuse is turned off, clean up any previous instance.
        if (!reuseServer && this.pig != null) {
            shutdown();
        }

        // Handle existing results.
        String scriptName = script.getScriptName();
        String resultsFile = hdfsWorkingDirectory + "results-" + scriptName;
        try {
            if (getPig().existsFile(resultsFile)) {
                if (replaceExisting) {
                    getPig().deleteFile(resultsFile);
                    log.info(String.format("Previous results deleted (%s)", resultsFile));
                } else {
                    return null;
                }
            }
        } catch (IOException e) {
            log.error("Error while trying to access HDFS.", e);
        }

        // Load input data.
        try {
            loadQuads(inputFile);
        } catch (IOException e) {
            log.error("Error while trying to load input data.", e);
            return null;
        }

        // Register script.
        try {
            StringBuilder jobName = new StringBuilder();
            jobName.append(scriptName).append(" / ")
                .append(inputFile.getFileSetIdentifier()).append(" / ")
                .append(inputFile.getTupleCount());
            getPig().setJobName(jobName.toString()); // doesn't seem to do anything.
            getPig().getPigContext().getProperties().setProperty(PigContext.JOB_NAME, jobName.toString());
            getPig().registerScript(script.getNewInputStream());
        } catch (IOException e) {
            log.error("Error while trying to load pig script.", e);
            return null;
        }

        // Execute job and store results.
        ExecJob job = null;
        try {
            log.debug("Starting execution of pig script.");
            String lastAlias = getPig().getPigContext().getLastAlias();
            job = getPig().store(lastAlias, resultsFile);
            log.debug("Finished execution of pig script.");
        } catch (IOException e) {
            log.error("Error while trying to execute pig script.", e);
            return null;
        }

        printHistory();
        return job.getStatistics();
    }

    /**
     * Prints the pig statement history to the DEBUG log.
     */
    private void printHistory() {
        // Redirect any output printed to System.out by PigServer.printHistory().
        PrintStream original = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
        this.pig.printHistory(true);
        System.setOut(original);

        StringBuilder sb = new StringBuilder();
        sb.append("Query history:\n");
        sb.append(new String(baos.toByteArray()));
        log.debug(sb.toString());
    }

    /**
     * Shuts down the pig server and deletes the instance.
     */
    public void shutdown() {
        if (this.pig != null) {
            this.log.info("Shutting down pig server.");
            this.pig.shutdown();
            this.pig = null;
        }
    }

    public enum HADOOP_LOCATION {HPI_CLUSTER, LOCALHOST}
}

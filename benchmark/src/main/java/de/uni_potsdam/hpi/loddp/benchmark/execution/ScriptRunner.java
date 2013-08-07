package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.Main;
import de.uni_potsdam.hpi.loddp.common.GraphvizUtil;
import de.uni_potsdam.hpi.loddp.common.HadoopLocation;
import de.uni_potsdam.hpi.loddp.common.PigContextUtil;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

public class ScriptRunner {

    protected static final Log log = LogFactory.getLog(ScriptRunner.class);
    private final String hdfsWorkingDirectory;
    private final HadoopLocation serverLocation;
    private final boolean reuseServer;
    private PigServer pig;
    private int resultLimit = -1;
    private boolean planPrinting = false;

    /**
     * Constructor.
     *
     * @param location             The hadoop server location.
     * @param hdfsWorkingDirectory The working directory for any HDFS-related commands. Datasets are loaded from, and
     *                             results written to directories relative to this path.
     */
    public ScriptRunner(HadoopLocation location, String hdfsWorkingDirectory) {
        this(location, false, hdfsWorkingDirectory);
    }

    /**
     * Constructor.
     *
     * @param location             The hadoop server location.
     * @param reuseServer          Whether to reuse the pig server, or create a new one for each invocation of {@link
     *                             #runScript}. If the pig server is reused, then calculations from previous scripts can
     *                             be reused as long as they have the same alias. This can also lead to errors, if two
     *                             aliases in different scripts represent different data sets.
     * @param hdfsWorkingDirectory The working directory for any HDFS-related commands. Datasets are loaded from, and
     *                             results written to directories relative to this path.
     */
    public ScriptRunner(HadoopLocation location, boolean reuseServer, String hdfsWorkingDirectory) {
        this.reuseServer = reuseServer;

        this.serverLocation = location;
        log.info("ScriptRunner is connecting to: " + location);

        // Make sure hdfs directory ends with a slash.
        if (hdfsWorkingDirectory.isEmpty()) hdfsWorkingDirectory = "./";
        else if (!hdfsWorkingDirectory.endsWith("/")) hdfsWorkingDirectory += '/';
        this.hdfsWorkingDirectory = hdfsWorkingDirectory;

        log.info(String.format("ScriptRunner is using %s PigServer(s) for executing scripts.",
            reuseServer ? "ONE" : "SEPARATE"));
    }

    public void enablePlanPrinting() {
        this.planPrinting = true;
    }

    public void disablePlanPrinting() {
        this.planPrinting = false;
    }

    /**
     * Initialises the {@link #pig} field.
     *
     * @throws IOException
     */
    private void initialisePig() throws IOException {
        PigContext context = PigContextUtil.getContext(serverLocation);
        this.pig = new PigServer(context);
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
     * Automatically limit all results to the given size.
     *
     * Theoretically, this could help pig throw away intermediate results earlier and thereby speed up execution of some
     * scripts. If nothing else, it should at least speed up the final sorting step.
     *
     * @param resultLimit The maximum number of results to store.
     */
    public void setResultLimit(int resultLimit) {
        this.resultLimit = resultLimit;
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

        applyResultLimit();

        // Print Operator plans.
        if (this.planPrinting) {
            try {
                String lastAlias = getPig().getPigContext().getLastAlias();
                String baseFilename = generateOperatorPlanBaseFilename(script);
                printOperatorPlans(lastAlias, baseFilename);
            } catch (IOException e) {
                log.error("Error while trying to print operator plans.", e);
            }
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
     * Print the logical, physical and mapreduce operator plans using Pig's EXPLAIN feature. Plans are printed as DOT
     * graphs and automatically converted to PNGs.
     *
     * @throws IOException
     */
    protected void printOperatorPlans(String alias, String baseOutputFilename) throws IOException {
        File logical = new File(baseOutputFilename + "logical.dot");
        logical.getParentFile().mkdirs();
        File physical = new File(baseOutputFilename + "physical.dot");
        File mapreduce = new File(baseOutputFilename + "mapreduce.dot");

        getPig().explain(alias, "dot", false, false,
            new PrintStream(logical), new PrintStream(physical), new PrintStream(mapreduce));

        GraphvizUtil.convertToImage("png", logical);
        GraphvizUtil.convertToImage("png", physical);
        GraphvizUtil.convertToImage("png", mapreduce);
    }

    /**
     * The common base name to use for graphs printed by {@link #printOperatorPlans(String, String)}
     */
    protected String generateOperatorPlanBaseFilename(PigScript script) {
        return new StringBuilder()
            .append(Main.getJobGraphDirectory())
            .append(script.getScriptName()).append("-").toString();
    }

    /**
     * Limit the number of entries in the result set to the size specified in the {@link #resultLimit} field by adding a
     * "result = LIMIT alias k;" statement to the script being executed.
     */
    private void applyResultLimit() {
        if (this.resultLimit <= 0) return;
        try {
            String lastAlias = getPig().getPigContext().getLastAlias();
            StringBuilder sb = new StringBuilder();
            sb.append(lastAlias).append("_limited")
                .append(" = LIMIT ").append(lastAlias).append(" ")
                .append(resultLimit).append(";");
            getPig().registerQuery(sb.toString());
        } catch (IOException e) {
            log.error(String.format("Error while trying to limit the result size to %d entries.", resultLimit), e);
        }
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

}

package de.uni_potsdam.hpi.loddp.benchmark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.IOException;

public class ScriptRunner {

    protected static final Log log = LogFactory.getLog(ScriptRunner.class);
    private PigServer pig;
    private boolean reuseServer;

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
    public ScriptRunner(boolean reuseServer) {
        this.reuseServer = reuseServer;
    }

    /**
     * Initialises the {@link #pig} field.
     *
     * @throws IOException
     */
    private void initialisePig() throws IOException {
        this.pig = new PigServer(ExecType.MAPREDUCE);

        // Register UDF + required libraries.
        this.pig.registerJar("ldif-single-0.5.1-jar-with-dependencies.jar");
        this.pig.registerJar("loddesc-core-0.1.jar");

        this.log.info("Created new pig server.");
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
     * @param script A pig script.
     */
    public PigStats runScript(PigScript script) {
        return runScript(script, true);
    }

    /**
     * Execute the given pig script.
     *
     * @param script          A pig script.
     * @param replaceExisting Whether to override existing results.
     */
    public PigStats runScript(PigScript script, boolean replaceExisting) {
        // If server reuse is turned off, clean up any previous instance.
        if (!reuseServer && this.pig != null) {
            shutdown();
        }

        // Handle existing results
        String resultsFile = "results-" + script.getScriptName();
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

        // Register script
        try {
            getPig().setJobName(script.getScriptName());
            getPig().registerScript(script.getInputStream());
        } catch (IOException e) {
            log.error("Error while trying to load pig script.", e);
            return null;
        }

        // Execute job and store results.
        ExecJob job = null;
        try {
            job = getPig().store(script.getResultAlias(), resultsFile);
        } catch (IOException e) {
            log.error("Error while trying to execute pig script.", e);
            return null;
        }

        // @todo maybe use this.pig.printHistory(true) to show complete script in order

        return job.getStatistics();
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

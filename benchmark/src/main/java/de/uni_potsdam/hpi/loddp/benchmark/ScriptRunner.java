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

    /**
     * Constructor.
     *
     * @throws IOException
     */
    public ScriptRunner() throws IOException {
        this.pig = new PigServer(ExecType.MAPREDUCE);

        // Register UDF + required libraries.
        this.pig.registerJar("ldif-single-0.5.1-jar-with-dependencies.jar");
        this.pig.registerJar("loddesc-core-0.1.jar");
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
        String resultsFile = "results-" + script.getScriptName();

        // Handle existing results
        try {
            if (this.pig.existsFile(resultsFile)) {
                if (replaceExisting) {
                    this.pig.deleteFile(resultsFile);
                    log.info(String.format("Previous results deleted (%s)", resultsFile));
                } else {
                    return null;
                }
            }
        } catch (IOException e) {
            log.error("Error while trying to access HDFS.", e);
        }

        // Register script
        this.pig.setJobName(script.getScriptName());
        try {
            this.pig.registerScript(script.getInputStream());
        } catch (IOException e) {
            log.error("Error while trying to load pig script.", e);
            return null;
        }

        // Execute job and store results.
        ExecJob job = null;
        try {
            job = this.pig.store(script.getResultAlias(), resultsFile);
        } catch (IOException e) {
            log.error("Error while trying to execute pig script.", e);
            return null;
        }

        return job.getStatistics();
    }

    public void shutdown() {
        if (this.pig != null) {
            this.pig.shutdown();
        }
    }

    /* output
    use this.pig.printHistory(true) -> shows complete script in order
     */

}

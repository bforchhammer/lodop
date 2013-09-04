package de.uni_potsdam.hpi.loddp.common.execution;

import de.uni_potsdam.hpi.loddp.common.HadoopLocation;
import de.uni_potsdam.hpi.loddp.common.PigContextUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import java.io.IOException;
import java.util.List;

/**
 * Base functionality for executing a PIG script on a Hadoop cluster.
 */
public class BasePigRunner implements PigRunner {

    private static final Log log = LogFactory.getLog(BasePigRunner.class);
    private final HadoopLocation serverLocation;
    private PigContext pigContext;
    private boolean replaceExistingResults = true;
    private HDFSFacade HDFS = new HDFSFacade();
    private String jobName = null;

    /**
     * Constructor.
     *
     * @param location The hadoop server location.
     */
    public BasePigRunner(HadoopLocation location) {
        this.serverLocation = location;
        log.info("ScriptRunner is connecting to: " + location);
    }

    /**
     * Whether to simply delete previous results, or fail with an exception if they exist already.
     *
     * @param value TRUE to delete existing results (default), FALSE to keep them and skip execution.
     */
    @Override
    public void setReplaceExistingResults(boolean value) {
        this.replaceExistingResults = value;
    }

    /**
     * Retrieve the pig context. If none has been created yet, builds a new one and tries to connect
     */
    @Override
    public PigContext getPigContext() throws PigRunnerException {
        if (this.pigContext == null) {
            try {
                this.pigContext = PigContextUtil.getContext(serverLocation);
                this.pigContext.connect();
            } catch (IOException e) {
                throw new PigRunnerException("Failed to build pig context.", e);
            }
        }
        return this.pigContext;
    }

    /**
     * Reset the stored hadoop job name for upcoming jobs.
     */
    @Override
    public void resetJobName() {
        this.jobName = null;
    }

    /**
     * Get the stored hadoop job name.
     *
     * @return
     */
    protected String getJobName() {
        return this.jobName;
    }

    /**
     * Set the hadoop job name for upcoming jobs.
     *
     * @param jobName
     */
    @Override
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     * Get the stored hadoop job name. If it is empty, fall back to a constructed job name based on the given
     * ScriptCompiler.
     *
     * @param compiler
     *
     * @return
     *
     * @throws PigRunnerException
     */
    protected String getJobName(ScriptCompiler compiler) throws PigRunnerException {
        if (jobName == null || jobName.isEmpty()) {
            jobName = buildJobName(compiler);
        }
        return jobName;
    }

    /**
     * Constructs a job name for the given ScriptCompiler using the contained logical plan sources and sinks.
     *
     * @param compiler
     *
     * @return
     *
     * @throws PigRunnerException
     */
    protected String buildJobName(ScriptCompiler compiler) throws PigRunnerException {
        StringBuilder sb = new StringBuilder("loddp:");
        try {
            Operator source = compiler.getOptimizedLogicalPlan().getSources().get(0);
            if (source instanceof LOLoad) {
                sb.append((((LOLoad) source).getFileSpec().getFileName()));
            } else {
                sb.append("unknown-source");
            }

            sb.append(" -> ");

            List<Operator> sinks = compiler.getOptimizedLogicalPlan().getSinks();
            if (sinks.size() > 1) {
                sb.append(sinks.size()).append(" sinks");
            } else {
                Operator sink = sinks.get(0);
                if (sink instanceof LOStore) {
                    sb.append((((LOStore) sink).getFileSpec().getFileName()));
                } else {
                    sb.append("unknown-sink");
                }
            }
        } catch (ScriptCompilerException e) {
            throw new PigRunnerException("Failed to construct job name.", e);
        }

        return sb.toString();
    }

    /**
     * Executes the physical plan contained within the given ScriptCompiler.
     *
     * @param compiler
     *
     * @return
     *
     * @throws PigRunnerException
     */
    @Override
    public PigStats execute(ScriptCompiler compiler) throws PigRunnerException {
        MapReduceLauncher launcher = new MapReduceLauncher();

        try {
            // Check that input files exists and output directories can be written to.
            validateIO(compiler.getLogicalPlan());

            // Keep track of used PIG features (for statistical purposes).
            ScriptState.get().setScriptFeatures(compiler.getOptimizedLogicalPlan());

            // Set the Hadoop job name.
            String jobName = getJobName(compiler);
            getPigContext().getProperties().setProperty(PigContext.JOB_NAME, jobName);

            // Execute physical plan.
            PigStats statistics = launcher.launchPig(compiler.getPhysicalPlan(), jobName, getPigContext());

            // Cleanup job name.
            resetJobName();

            return statistics;
        } catch (Exception e) {
            throw new PigRunnerException("Failed to execute script", e);
        }
    }

    protected void validateIO(LogicalPlan logicalPlan) throws PigRunnerException {
        if (logicalPlan != null) {
            validateInput(logicalPlan);
            validateOutput(logicalPlan);
        }
    }

    /**
     * Checks that all inputs of the given logical plan point to valid files on the HDFS.
     *
     * @param logicalPlan The logical plan for which to check all sources.
     *
     * @throws PigRunnerException In case of validation error.
     */
    protected void validateInput(LogicalPlan logicalPlan) throws PigRunnerException {
        List<Operator> sources = logicalPlan.getSources();
        for (Operator source : sources) {
            if (source instanceof LOLoad) {
                LOLoad load = (LOLoad) source;
                String filename = load.getFileSpec().getFileName();
                validateInput(filename);
            } else {
                log.warn(String.format("Skipping validation of input, source operator unknown. (%s)",
                    source.getClass().getName()));
            }
        }
    }

    /**
     * Checks that the given filename exists on the DFS.
     *
     * @param filename The filename to check.
     *
     * @throws PigRunnerException In case of validation error.
     */
    protected void validateInput(String filename) throws PigRunnerException {
        try {
            if (!HDFS.exists(filename)) {
                throw new PigRunnerException(String.format("Input file does not exist on HDFS (%s).", filename));
            }
        } catch (IOException e) {
            throw new PigRunnerException("Failed to check input file on HDFS.", e);
        }
    }

    /**
     * Checks that all outputs of the given logical plan can be written to on the HDFS.
     *
     * @param logicalPlan The logical plan for which to check all sinks.
     *
     * @throws PigRunnerException In case of validation error.
     */
    protected void validateOutput(LogicalPlan logicalPlan) throws PigRunnerException {
        List<Operator> sinks = logicalPlan.getSinks();
        for (Operator sink : sinks) {
            if (sink instanceof LOStore) {
                LOStore store = (LOStore) sink;
                String filename = store.getFileSpec().getFileName();
                validateOutput(filename);
            } else {
                log.warn(String.format("Skipping validation of output, sink operator unknown. (%s)",
                    sink.getClass().getName()));
            }
        }
    }

    /**
     * Checks that the given filename either does not exist, or can be deleted.
     *
     * @param filename The filename to check.
     *
     * @throws PigRunnerException In case of validation error.
     */
    protected void validateOutput(String filename) throws PigRunnerException {
        try {
            if (HDFS.exists(filename)) {
                if (this.replaceExistingResults) {
                    HDFS.delete(filename);
                    log.info(String.format("Previous output directory deleted (%s).", filename));
                } else {
                    throw new PigRunnerException(String.format("Output directory already exists (%s).", filename));
                }
            }
        } catch (IOException e) {
            throw new PigRunnerException("Failed to check output directory on HDFS.", e);
        }
    }

    /**
     * A small utility wrapper to ease file manipulation on the HDFS.
     */
    protected class HDFSFacade {
        public boolean exists(String filename) throws IOException, PigRunnerException {
            return getPigContext().getDfs().asElement(filename).exists();
        }

        public void delete(String filename) throws IOException, PigRunnerException {
            getPigContext().getDfs().asElement(filename).delete();
        }
    }
}

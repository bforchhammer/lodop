package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.reporting.ExecutionStats;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunner;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunnerException;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompilerException;
import de.uni_potsdam.hpi.loddp.common.printing.GraphvizDumper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * Decorating wrapper for {@link de.uni_potsdam.hpi.loddp.common.execution.PigRunner} which is adapted for the benchmark
 * application.
 *
 * E.g. the execute method returns an {@link ExecutionStats} instance instead of PigStats.
 */
public class PigScriptRunner implements PigRunner {
    private static final Log log = LogFactory.getLog(PigScriptRunner.class);
    private PigRunner decorated;
    private String explainOutputDirectory;
    private boolean explainPlans = false;

    public PigScriptRunner(PigRunner decorated) {
        this.decorated = decorated;
    }

    public void setExplainOutputDirectory(String explainOutputDirectory) {
        this.explainOutputDirectory = explainOutputDirectory;
    }

    public void setExplainPlans(boolean explainPlans) {
        this.explainPlans = explainPlans;
    }

    @Override
    public void setReplaceExistingResults(boolean value) {
        decorated.setReplaceExistingResults(value);
    }

    @Override
    public PigContext getPigContext() throws PigRunnerException {
        return decorated.getPigContext();
    }

    @Override
    public void resetJobName() {
        decorated.resetJobName();
    }

    @Override
    public void setJobName(String jobName) {
        decorated.setJobName(jobName);
    }

    /**
     * @deprecated If possible, {@link #execute(ScriptCompiler, String, InputFile)} should be called instead.
     */
    @Override
    @Deprecated
    public PigStats execute(ScriptCompiler compiler) throws PigRunnerException {
        return decorated.execute(compiler);
    }

    public final ScriptStats execute(ScriptCompiler compiler, String scriptName,
                                     InputFile file) throws PigRunnerException {
        beforeExecute(compiler, scriptName, file);
        ScriptStats stats = doExecute(compiler, scriptName, file);
        afterExecute(compiler, scriptName, file, stats);
        return stats;
    }

    protected void beforeExecute(ScriptCompiler compiler, String scriptName, InputFile file) {
        log.info(String.format("Starting script execution: %s on %s / %s", scriptName, file.getFileSetIdentifier(),
            file.getTupleCount()));
    }

    protected ScriptStats doExecute(ScriptCompiler compiler, String scriptName, InputFile file) throws PigRunnerException {
        setJobName(scriptName + " / " + file.getFileSetIdentifier() + " / " + file.getTupleCount());
        PigStats stats = decorated.execute(compiler);
        return new ExecutionStats(file, stats, scriptName);
    }

    protected void afterExecute(ScriptCompiler compiler, String scriptName, InputFile file, ScriptStats stats) {
        if (explainPlans) {
            explainPlans(compiler, scriptName);
        }
        if (stats instanceof ExecutionStats) {
            ((ExecutionStats) stats).printStats();
        }
        log.info("Finished script execution.");
    }

    public void explainPlans(ScriptCompiler compiler, String scriptName) {
        GraphvizDumper dumper = new GraphvizDumper(explainOutputDirectory + scriptName + '/');

        // We only want to do this once for each script, therefore check whether the output directory exists already.
        if (dumper.outputExists()) {
            log.debug("Skipped dumping of operator plans because graphs seem to exist already.");
            return;
        }

        try {
            log.info("Dumping plans as graphs.");
            dumper.print(compiler.getLogicalPlan(), "-unoptimized");
            dumper.print(compiler.getOptimizedLogicalPlan(), "-optimized");
            dumper.print(compiler.getPhysicalPlan());
            dumper.print(compiler.getMapReducePlan());
        } catch (ScriptCompilerException e) {
            log.error(String.format("Failed to print plans for script %s.", scriptName), e);
        }
    }
}

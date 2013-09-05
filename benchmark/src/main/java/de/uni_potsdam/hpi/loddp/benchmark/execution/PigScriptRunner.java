package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.reporting.ExecutionStats;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.GraphvizUtil;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunner;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunnerException;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompilerException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.DotPOPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.DotLOPrinter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

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
        try {
            // We only want to this once, we therefore check whether the output directory exists already.
            if (!getDotOutputDirectory(scriptName).exists()) {
                log.info("Dumping plans as graphs.");
                explainPlan(compiler.getLogicalPlan(), getDotOutputFile(scriptName, "logical"));
                explainPlan(compiler.getOptimizedLogicalPlan(), getDotOutputFile(scriptName, "logical-optimized"));
                explainPlan(compiler.getPhysicalPlan(), getDotOutputFile(scriptName, "physical"));
                explainPlan(compiler.getMapReducePlan(), getDotOutputFile(scriptName, "map-reduce"));
            }
        } catch (IOException e) {
            log.error(String.format("Failed to print plans for script %s.", scriptName), e);
        } catch (ScriptCompilerException e) {
            log.error(String.format("Failed to print plans for script %s.", scriptName), e);
        }
    }

    private File getDotOutputFile(String scriptName, String planType) {
        File directory = getDotOutputDirectory(scriptName);
        return new File(directory, planType + ".dot");
    }

    private File getDotOutputDirectory(String scriptName) {
        return new File(explainOutputDirectory + scriptName + "/");
    }

    private void explainPlan(Object plan, File dotFile) throws IOException {
        if (plan == null) return;
        dotFile.getParentFile().mkdirs();
        PrintStream ps = new PrintStream(dotFile);
        if (plan instanceof LogicalPlan) {
            new DotLOPrinter((LogicalPlan) plan, ps).dump();
        } else if (plan instanceof PhysicalPlan) {
            new DotPOPrinter((PhysicalPlan) plan, ps).dump();
        } else if (plan instanceof MROperPlan) {
            new DotMRPrinter((MROperPlan) plan, ps).dump();
        } else {
            throw new IllegalArgumentException("Expected plan parameter to be an object of type LogicalPlan, " +
                "PhysicalPlan, or MROperPlan . Received " + plan.getClass().getName() + " instead.");
        }
        GraphvizUtil.convertToImage("png", dotFile);
    }
}

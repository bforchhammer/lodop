package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.common.HadoopLocation;
import de.uni_potsdam.hpi.loddp.common.execution.BasePigRunner;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunner;
import de.uni_potsdam.hpi.loddp.optimization.PlanOptimizerBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ScriptRunnerBuilder {
    private static final Log log = LogFactory.getLog(ScriptRunnerBuilder.class);
    private HadoopLocation location = HadoopLocation.LOCALHOST;
    private int repeat = 1;
    private String hdfsOutputDirectory = "";
    private String explainOutputDirectory = "plans/";
    private boolean merged = false;
    private boolean optimizeMerged = false;
    private boolean optimizerCombineForeachs = true;
    private boolean optimizerCombineFilters = true;
    private boolean optimizerIgnoreProjections = true;
    private boolean replaceExistingResults = true;
    private boolean explainPlans = false;

    public void setLocation(HadoopLocation location) {
        this.location = location;
    }

    public void setRepeat(int repeat) {
        if (repeat < 1) {
            log.warn("--repeat argument ignored because the specified value was negative or zero (positive " +
                "integer expected).");
        } else {
            this.repeat = repeat;
            log.info(String.format("Repeating each script %d times.", repeat));
        }
    }

    public void setHdfsOutputDirectory(String hdfsOutputDirectory) {
        this.hdfsOutputDirectory = normalizePath(hdfsOutputDirectory);
    }

    private String normalizePath(String path) {
        if (path == null || path.isEmpty()) return "";
        if (!path.endsWith("/")) path += '/';
        return path;
    }

    public void setMerged(boolean merged) {
        this.merged = merged;
        if (merged) {
            log.info("Scripts are merged into one large plan.");
        }
    }

    public void setOptimizeMerged(boolean optimizeMerged) {
        this.optimizeMerged = optimizeMerged;
        if (optimizeMerged) {
            log.info("Merged scripts are optimized.");
        }
    }

    public void setOptimizerCombineForeachs(boolean optimizerCombineForeachs) {
        setOptimizeMerged(true);
        this.optimizerCombineForeachs = optimizerCombineForeachs;
    }

    public void setOptimizerCombineFilters(boolean optimizerCombineFilters) {
        setOptimizeMerged(true);
        this.optimizerCombineFilters = optimizerCombineFilters;
    }

    public void setOptimizerIgnoreProjections(boolean optimizerIgnoreProjections) {
        setOptimizeMerged(true);
        this.optimizerIgnoreProjections = optimizerIgnoreProjections;
    }

    public void setReplaceExistingResults(boolean replaceExistingResults) {
        this.replaceExistingResults = replaceExistingResults;
    }

    public void setExplainPlans(boolean explainPlans) {
        this.explainPlans = explainPlans;
    }

    public void setExplainOutputDirectory(String explainOutputDirectory) {
        this.explainOutputDirectory = normalizePath(explainOutputDirectory);
    }

    private PigScriptRunner buildPigRunner() {
        // Create basic pig runner.
        PigRunner pigRunner = new BasePigRunner(location);

        // Create benchmark-specific pig runner
        PigScriptRunner runner;
        if (repeat > 1) {
            runner = new RepeatedPigScriptRunner(pigRunner, repeat);
        } else {
            runner = new PigScriptRunner(pigRunner);
        }

        runner.setReplaceExistingResults(replaceExistingResults);

        if (explainPlans) {
            runner.setExplainPlans(true);
            runner.setExplainOutputDirectory(explainOutputDirectory);
        }

        return runner;
    }

    public ScriptRunner build() {
        PigScriptRunner pigScriptRunner = buildPigRunner();
        ScriptRunner scriptRunner;
        if (merged) {
            scriptRunner = new MergedScriptRunner(pigScriptRunner, hdfsOutputDirectory, getCustomOptimizer());
        } else {
            scriptRunner = new DefaultScriptRunner(pigScriptRunner, hdfsOutputDirectory);
        }
        return scriptRunner;
    }

    protected PlanOptimizerBuilder getCustomOptimizer() {
        PlanOptimizerBuilder builder = null;
        if (optimizeMerged) {
            builder = new PlanOptimizerBuilder();
            builder.setCombineFilters(optimizerCombineFilters);
            builder.setCombineForeachs(optimizerCombineForeachs);
            builder.setIgnoreProjections(optimizerIgnoreProjections);
        }
        return builder;
    }
}

package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunnerException;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompilerException;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import de.uni_potsdam.hpi.loddp.optimization.PlanOptimizerBuilder;
import de.uni_potsdam.hpi.loddp.optimization.merging.LogicalPlanMerger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.optimizer.PlanOptimizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Script runner which merges multiple scripts into one large logical plan before execution.
 */
public class MergedScriptRunner extends AbstractScriptRunner {

    protected static final Log log = LogFactory.getLog(MergedScriptRunner.class);
    private boolean mergeOptimizedPlans = false;
    private PlanOptimizerBuilder customOptimizer;

    public MergedScriptRunner(PigScriptRunner pigScriptRunner, String hdfsOutputDirectory) {
        this(pigScriptRunner, hdfsOutputDirectory, null);
    }

    public MergedScriptRunner(PigScriptRunner pigScriptRunner, String hdfsOutputDirectory, PlanOptimizerBuilder customOptimizer) {
        super(pigScriptRunner, hdfsOutputDirectory);
        this.customOptimizer = customOptimizer;
    }

    public void setMergeOptimizedPlans(boolean mergeOptimizedPlans) {
        this.mergeOptimizedPlans = mergeOptimizedPlans;
    }

    public void setCustomOptimizer(PlanOptimizerBuilder customOptimizer) {
        this.customOptimizer = customOptimizer;
    }

    /**
     * @todo Fix support for changing the input filename on a merged script.
     */
    @Override
    public List<ScriptStats> execute(Iterable<PigScript> scripts, Iterable<InputFile> files) {
        List<ScriptStats> stats = new ArrayList<ScriptStats>();
        Iterator<InputFile> filesIterator = files.iterator();

        ScriptCompiler compiler = null;
        while (filesIterator.hasNext()) {
            InputFile file = filesIterator.next();
            try {
                // Merge all scripts together the first time; for later iterations simply update the input filename.
                /*if (compiler == null) {
                    compiler = mergeScripts(scripts, file);
                } else {
                    compiler.updateFilename(getInputFilename(file));
                }*/
                compiler = mergeScripts(scripts, file);
                // Execute the merged script.
                stats.add(execute(compiler, "merged-scripts", file));
            } /*catch (ScriptCompilerException e) {
                log.error("Failed to merge scripts.", e);
            } */ catch (PigRunnerException e) {
                log.error("Failed to merge scripts.", e);
            }
        }

        return stats;
    }

    private ScriptCompiler mergeScripts(Iterable<PigScript> scripts, InputFile file) throws PigRunnerException {
        LogicalPlanMerger merger = new LogicalPlanMerger();
        for (PigScript script : scripts) {
            try {
                ScriptCompiler compiler = getCompiler(script, file);
                if (mergeOptimizedPlans) {
                    merger.merge(compiler.getOptimizedLogicalPlan());
                } else {
                    merger.merge(compiler.getLogicalPlan());
                }
            } catch (ScriptCompilerException e) {
                log.error("Failed to compile script " + script.getScriptName(), e);
            } catch (PigRunnerException e) {
                log.error("Failed to merge script " + script.getScriptName(), e);
            } catch (FrontendException e) {
                log.error("Failed to merge script " + script.getScriptName(), e);
            }
        }

        // Apply custom optimization rules if set.
        if (customOptimizer != null) {
            PlanOptimizer optimizer = customOptimizer.getInstance(merger.getMergedPlan());
            try {
                optimizer.optimize();
            } catch (FrontendException e) {
                throw new PigRunnerException("Failed to optimize merged plan.", e);
            }
        }

        return new ScriptCompiler(pigScriptRunner.getPigContext(), merger.getMergedPlan(), mergeOptimizedPlans);
    }
}

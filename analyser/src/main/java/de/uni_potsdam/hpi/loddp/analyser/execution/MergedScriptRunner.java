package de.uni_potsdam.hpi.loddp.analyser.execution;

import de.uni_potsdam.hpi.loddp.analyser.merging.MergedLogicalPlan;
import de.uni_potsdam.hpi.loddp.common.ScriptCompiler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import java.io.IOException;

/**
 * Experimental class for executing a merged logical plan.
 */
public class MergedScriptRunner {
    protected static final Log log = LogFactory.getLog(MergedScriptRunner.class);
    PigContext pigContext;

    public MergedScriptRunner(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    public static PigStats execute(MergedLogicalPlan plan, PigContext pigContext) {
        try {
            MergedScriptRunner runner = new MergedScriptRunner(pigContext);
            PigStats stats = runner.execute(plan, "merged-plan");
            return stats;
        } catch (IOException e) {
            log.error("Could not execute merged plan.", e);
        }
        return null;
    }

    public PigStats execute(MergedLogicalPlan plan, String jobName) throws ExecException, FrontendException {
        ScriptState.get().setScriptFeatures(plan); // keep track of used PIG features.
        ScriptCompiler compiler = new ScriptCompiler(pigContext, plan, false);
        MapReduceLauncher launcher = new MapReduceLauncher();

        // Execute physical plan and return stats.
        try {
            return launcher.launchPig(compiler.getPhysicalPlan(), jobName, pigContext);
        } catch (FrontendException e) {
            throw e;
        } catch (ExecException e) {
            throw e;
        } catch (Exception e) {
            throw new FrontendException(e);
        }
    }
}

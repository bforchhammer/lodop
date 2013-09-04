package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.reporting.RepeatedExecutionStats;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunner;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunnerException;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Allows to execute scripts repeatedly and merge their execution statistics.
 */
public class RepeatedPigScriptRunner extends de.uni_potsdam.hpi.loddp.benchmark.execution.PigScriptRunner {
    private static final Log log = LogFactory.getLog(RepeatedPigScriptRunner.class);
    private int repeat;

    public RepeatedPigScriptRunner(PigRunner decorated, int repeat) {
        super(decorated);
        this.repeat = repeat;
    }

    @Override
    protected ScriptStats doExecute(ScriptCompiler compiler, String scriptName,
                                    InputFile file) throws PigRunnerException {
        RepeatedExecutionStats stats = new RepeatedExecutionStats(file, scriptName);
        for (int i = 0; i < repeat; i++) {
            log.info(String.format(" > Iteration %d of %d.", i + 1, repeat));
            compiler.resetPhysicalPlan(); // For each new iteration, all output files need to be removed in order for
            // the script to run properly. This includes output directories as well as temporary files (which are very
            // likely to occur with merged, multi-output plans).
            stats.add(super.doExecute(compiler, scriptName, file));
        }
        return stats;
    }
}

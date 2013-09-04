package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunner;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunnerException;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The default script runner class.
 */
public class DefaultScriptRunner extends AbstractScriptRunner {
    private static final Log log = LogFactory.getLog(DefaultScriptRunner.class);

    public DefaultScriptRunner(PigScriptRunner pigScriptRunner, String hdfsWorkingDirectory) {
        super(pigScriptRunner, hdfsWorkingDirectory);
    }

    @Override
    public List<ScriptStats> execute(Iterable<PigScript> scripts, Iterable<InputFile> files) {
        List<ScriptStats> stats = new ArrayList<ScriptStats>();
        for (PigScript script : scripts) {
            for (InputFile file : files) {
                try {
                    ScriptCompiler compiler = getCompiler(script, file);
                    stats.add(this.execute(compiler, script.getScriptName(), file));
                } catch (PigRunnerException e) {
                    log.error(String.format("Failed to execute pig script %s on %s / %d.", script.getScriptName(),
                        file.getFileSetIdentifier(), file.getTupleCount()));
                }
            }
        }
        return stats;
    }
}

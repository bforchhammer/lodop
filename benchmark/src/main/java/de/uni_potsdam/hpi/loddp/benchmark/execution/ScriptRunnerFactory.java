package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.common.HadoopLocation;
import de.uni_potsdam.hpi.loddp.common.execution.BasePigRunner;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunner;

/**
 * Factory for PigScriptRunner and ScriptRunner instances.
 */
public class ScriptRunnerFactory {

    public static PigScriptRunner getPigRunner(HadoopLocation location, int repeat) {
        PigRunner runner = new BasePigRunner(location);
        if (repeat > 1) {
            return new RepeatedPigScriptRunner(runner, repeat);
        }
        return new de.uni_potsdam.hpi.loddp.benchmark.execution.PigScriptRunner(runner);
    }

    public static ScriptRunner getScriptRunner(HadoopLocation location, String hdfsWorkingDirectory, boolean merged,
                                               int repeat) {
        PigScriptRunner pigScriptRunner = getPigRunner(location, repeat);
        if (merged) {
            return new MergedScriptRunner(pigScriptRunner, hdfsWorkingDirectory);
        }
        return new DefaultScriptRunner(pigScriptRunner, hdfsWorkingDirectory);
    }
}

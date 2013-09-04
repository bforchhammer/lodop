package de.uni_potsdam.hpi.loddp.common.execution;

import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;

public interface PigRunner {
    void setReplaceExistingResults(boolean value);

    PigContext getPigContext() throws PigRunnerException;

    void resetJobName();

    void setJobName(String jobName);

    PigStats execute(ScriptCompiler compiler) throws PigRunnerException;
}

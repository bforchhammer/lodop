package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;

import java.util.List;

/**
 * Interface for ScriptRunners which are compatible with the benchmark application.
 *
 * This means being able to handle {@link InputFile} instances, returning {@link ScriptStats} objects, and catching any
 * errors thrown in the process.
 */
public interface ScriptRunner {

    /**
     * Execute the given list of scripts on the given list of input files.
     *
     * @param scripts An iterable set of scripts to execute.
     * @param files   A list of input files.
     *
     * @return A list of script execution statistics.
     */
    public List<ScriptStats> execute(Iterable<PigScript> scripts, Iterable<InputFile> files);
}

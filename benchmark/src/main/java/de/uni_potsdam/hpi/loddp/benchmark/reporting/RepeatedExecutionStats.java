package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import de.uni_potsdam.hpi.loddp.benchmark.execution.InputFile;
import org.apache.pig.tools.pigstats.JobStats;

import java.util.LinkedList;
import java.util.List;

/**
 * Class for collecting statistics for multiple identical runs.
 *
 * Class methods output the average of all contained ExecutionStats.
 */
public class RepeatedExecutionStats implements ScriptStats {

    private final InputFile inputFile;
    private final String scriptName;
    private final List<ScriptStats> stats;

    public RepeatedExecutionStats(InputFile inputFile, String scriptName) {
        this.inputFile = inputFile;
        this.scriptName = scriptName;

        this.stats = new LinkedList<ScriptStats>();
    }

    public void add(ScriptStats stats) {
        this.stats.add(stats);
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public String getDatasetIdentifier() {
        return inputFile.getFileSetIdentifier();
    }

    @Override
    public long getInputSize() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getInputSize();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getOutputSize() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getOutputSize();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimeTotal() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getTimeTotal();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public int getNumberJobs() {
        int total = 0;
        for (ScriptStats s : stats) {
            total += s.getNumberJobs();
        }
        return Math.round(total / stats.size());
    }

    /**
     * Returns JobStats for the "first" ExecutionStats instance.
     *
     * @todo This should probably try to summarize and return job stats across all executions instead of just one.
     */
    @Override
    public List<JobStats> getJobStats() {
        return stats.get(0).getJobStats();
    }

    @Override
    public int getNumberMapsTotal() {
        int total = 0;
        for (ScriptStats s : stats) {
            total += s.getNumberMapsTotal();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public int getNumberReducesTotal() {
        int total = 0;
        for (ScriptStats s : stats) {
            total += s.getNumberReducesTotal();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimePig() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getTimePig();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimeMapReduce() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getTimeMapReduce();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimeMapReduceJobSetup() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getTimeMapReduceJobSetup();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimeMapReduceJobCleanup() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getTimeMapReduceJobCleanup();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimeMap() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getTimeMap();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimeReduce() {
        long total = 0;
        for (ScriptStats s : stats) {
            total += s.getTimeReduce();
        }
        return Math.round(total / stats.size());
    }
}

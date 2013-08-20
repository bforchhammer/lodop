package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import de.uni_potsdam.hpi.loddp.benchmark.execution.InputFile;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
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
    private final PigScript pigScript;
    private final List<ExecutionStats> stats;

    public RepeatedExecutionStats(InputFile inputFile, PigScript pigScript) {
        this.inputFile = inputFile;
        this.pigScript = pigScript;

        this.stats = new LinkedList<ExecutionStats>();
    }

    public void add(ExecutionStats stats) {
        this.stats.add(stats);
    }

    @Override
    public String getScriptName() {
        return pigScript.getScriptName();
    }

    @Override
    public String getDatasetIdentifier() {
        return inputFile.getFileSetIdentifier();
    }

    @Override
    public long getInputSize() {
        long total = 0;
        for (ExecutionStats s : stats) {
            total += s.getInputSize();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getOutputSize() {
        long total = 0;
        for (ExecutionStats s : stats) {
            total += s.getOutputSize();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getTimeTotal() {
        long total = 0;
        for (ExecutionStats s : stats) {
            total += s.getTimeTotal();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public int getNumberJobs() {
        int total = 0;
        for (ExecutionStats s : stats) {
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
        for (ExecutionStats s : stats) {
            total += s.getNumberMapsTotal();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public int getNumberReducesTotal() {
        int total = 0;
        for (ExecutionStats s : stats) {
            total += s.getNumberReducesTotal();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getAvgMapTimeTotal() {
        long total = 0;
        for (ExecutionStats s : stats) {
            total += s.getAvgMapTimeTotal();
        }
        return Math.round(total / stats.size());
    }

    @Override
    public long getAvgReduceTimeTotal() {
        long total = 0;
        for (ExecutionStats s : stats) {
            total += s.getAvgReduceTimeTotal();
        }
        return Math.round(total / stats.size());
    }
}

package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import de.uni_potsdam.hpi.loddp.benchmark.Main;
import de.uni_potsdam.hpi.loddp.benchmark.execution.InputFile;
import de.uni_potsdam.hpi.loddp.common.GraphvizUtil;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobID;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Contains information about the execution/run of a pig script on a specific set of input data.
 */
public class ExecutionStats implements ScriptStats {

    protected static final Log log = LogFactory.getLog(ExecutionStats.class);
    private final InputFile inputFile;
    private final PigStats pigStats;
    private final String scriptName;
    private final long inputSize;
    private int numberMapsTotal = -1;
    private int numberReducesTotal = -1;
    private Long avgMapTimeTotal = -1L;
    private Long avgReduceTimeTotal = -1L;
    private int iterationNumber = -1;

    /**
     * Constructor.
     *
     * @param input
     * @param pigStats
     * @param scriptName
     */
    public ExecutionStats(InputFile input, PigStats pigStats, String scriptName) {
        this.inputFile = input;
        this.pigStats = pigStats;
        this.scriptName = scriptName;
        long inputSize = inputFile.getTupleCount();
        this.inputSize = (inputSize > 0) ? inputSize : this.pigStats.getInputStats().get(0).getNumberRecords();
    }

    public void setIterationNumber(int iterationNumber) {
        this.iterationNumber = iterationNumber;
    }

    public long getOutputSize() {
        return pigStats.getRecordWritten();
    }

    public long getInputSize() {
        return inputSize;
    }

    public long getTimeTotal() {
        return pigStats.getDuration();
    }

    public String getScriptName() {
        return scriptName;
    }

    public String getDatasetIdentifier() {
        return inputFile.getFileSetIdentifier();
    }

    /**
     * Computes values of {@link #numberMapsTotal} and {@link #numberReducesTotal}.
     */
    private void summarizeJobStats() {
        List<JobStats> arr = getJobStats();

        // Reset field values.
        numberMapsTotal = 0;
        numberReducesTotal = 0;

        Long mapTimeTotal = 0L;
        Long reduceTimeTotal = 0L;
        for (JobStats js : arr) {
            numberMapsTotal += js.getNumberMaps();
            numberReducesTotal += js.getNumberReduces();
            mapTimeTotal += js.getAvgMapTime() * js.getNumberMaps();
            reduceTimeTotal += js.getAvgREduceTime() * js.getNumberReduces();
        }

        avgMapTimeTotal = (numberMapsTotal > 0) ? mapTimeTotal / numberMapsTotal : 0;
        avgReduceTimeTotal = (numberReducesTotal > 0) ? reduceTimeTotal / numberReducesTotal : 0;
    }

    public List<JobStats> getJobStats() {
        return pigStats.getJobGraph().getSuccessfulJobs();
    }

    public int getNumberJobs() {
        return pigStats.getNumberJobs();
    }

    public int getNumberMapsTotal() {
        if (numberMapsTotal < 0) summarizeJobStats();
        return numberMapsTotal;
    }

    public int getNumberReducesTotal() {
        if (numberReducesTotal < 0) summarizeJobStats();
        return numberReducesTotal;
    }

    public long getAvgMapTimeTotal() {
        if (avgMapTimeTotal < 0) summarizeJobStats();
        return avgMapTimeTotal;
    }

    public long getAvgReduceTimeTotal() {
        if (avgReduceTimeTotal < 0) summarizeJobStats();
        return avgReduceTimeTotal;
    }

    private String buildPhaseStatistics() {
        StringBuilder time_sb = new StringBuilder("Time overview:\n");
        time_sb.append("JobId\tSetup\tMap\tReduces\tCleanup\tTotal\n");

        StringBuilder bytes_sb = new StringBuilder("I/O overview:\n");
        bytes_sb.append("JobId\tPhase\tHDFS read\tLocal read\tHDFS written\tLocal written\n");

        List<JobStats> jobs = pigStats.getJobGraph().getJobList();

        // map: map, combine?
        // reduce: shuffle, sort, reduce?
        long previousJobFinished = 0;
        for (JobStats js : jobs) {
            JobID jobId = JobID.forName(js.getJobId());
            try {
                TaskPhaseStatistics setup = TaskPhaseStatistics.getTaskPhaseStatistics("setup",
                    pigStats.getJobClient().getSetupTaskReports(jobId));
                TaskPhaseStatistics maps = TaskPhaseStatistics.getTaskPhaseStatistics("map",
                    pigStats.getJobClient().getMapTaskReports(jobId));
                TaskPhaseStatistics reduces = TaskPhaseStatistics.getTaskPhaseStatistics("reduce",
                    pigStats.getJobClient().getReduceTaskReports(jobId));
                TaskPhaseStatistics cleanups = TaskPhaseStatistics.getTaskPhaseStatistics("cleanup",
                    pigStats.getJobClient().getCleanupTaskReports(jobId));
                TaskPhaseStatistics[] allPhases = new TaskPhaseStatistics[] {setup, maps, reduces, cleanups};
                TaskPhaseStatistics total = new TaskPhaseStatistics("total", allPhases);

                // @todo this should consider the job DAG (not all jobs are sequential).
                // @todo also, we should include stats for when something was started from PIG.
                // @todo and best output the whole thing in a graph.
                if (previousJobFinished != 0) {
                    time_sb.append("Between jobs\t\t\t\t").append(total.getStartTime() - previousJobFinished).append("\n");
                }
                previousJobFinished = total.getFinishTime();

                time_sb.append(jobId.toString()).append("\t");
                for (TaskPhaseStatistics stat : allPhases) {
                    time_sb.append(stat.getDuration()).append("\t");
                }
                time_sb.append(total.getDuration()).append("\n");

                for (TaskPhaseStatistics stat : allPhases) {
                    bytes_sb.append(jobId.toString()).append("\t");
                    bytes_sb.append(stat.getLabel()).append("\t");
                    bytes_sb.append(stat.getHdfsBytesRead()).append("\t");
                    bytes_sb.append(stat.getLocalBytesRead()).append("\t");
                    bytes_sb.append(stat.getHdfsBytesWritten()).append("\t");
                    bytes_sb.append(stat.getLocalBytesWritten()).append("\n");
                }
                bytes_sb.append(jobId.toString()).append("\t");
                bytes_sb.append(total.getLabel()).append("\t");
                bytes_sb.append(total.getHdfsBytesRead()).append("\t");
                bytes_sb.append(total.getLocalBytesRead()).append("\t");
                bytes_sb.append(total.getHdfsBytesWritten()).append("\t");
                bytes_sb.append(total.getLocalBytesWritten()).append("\n");
            } catch (IOException e) {
                log.error("Failed to grab task reports for something.", e);
                time_sb.append(e.getMessage());
                bytes_sb.append(e.getMessage());
            }
        }
        time_sb.append("\n");
        time_sb.append(bytes_sb);
        time_sb.append("\n");
        return time_sb.toString();
    }

    public void printStats() {
        StringBuilder sb = new StringBuilder();

        sb.append("Statistics for ").append(getScriptName().toUpperCase()).append("\n");

        if (iterationNumber != -1) {
            sb.append("Iteration ").append(iterationNumber).append("\n");
        }

        InputStats is = pigStats.getInputStats().get(0);
        sb.append("Input: \t").append(is.getNumberRecords()).append(" records")
            .append(" (").append(is.getBytes()).append(" Bytes)")
            .append(" from DataSet ").append(inputFile.getFileSetIdentifier())
            .append(", filename = ").append(inputFile.getFilename()).append("\n");

        sb.append("Total time: \t").append(DurationFormatUtils.formatDurationHMS(pigStats.getDuration())).append("\n");

        sb.append(buildPhaseStatistics());

        sb.append("JobId\tMaps\tReduces\tMaxMapTime\tMinMapTime\tAvgMapTime\tMaxReduceTime\t" +
            "MinReduceTime\tAvgReduceTime\tAlias\tFeature\tOutputs\n");
        List<JobStats> arr = pigStats.getJobGraph().getJobList();
        for (JobStats js : arr) {
            String id = (js.getJobId() == null) ? "N/A" : js.getJobId();
            if (js.getState() == JobStats.JobState.FAILED) {
                sb.append(id).append("\t")
                    .append(js.getAlias()).append("\t")
                    .append(js.getFeature()).append("\t")
                    .append("Message: ").append(js.getErrorMessage()).append("\t");
            } else if (js.getState() == JobStats.JobState.SUCCESS) {
                sb.append(id).append("\t")
                    .append(js.getNumberMaps()).append("\t")
                    .append(js.getNumberReduces()).append("\t");
                if (js.getMaxMapTime() < 0) {
                    sb.append("n/a\t").append("n/a\t").append("n/a\t");
                } else {
                    sb.append(DurationFormatUtils.formatDurationHMS(js.getMaxMapTime())).append("\t")
                        .append(DurationFormatUtils.formatDurationHMS(js.getMinMapTime())).append("\t")
                        .append(DurationFormatUtils.formatDurationHMS(js.getAvgMapTime())).append("\t");
                }
                if (js.getMaxReduceTime() < 0) {
                    sb.append("n/a\t").append("n/a\t").append("n/a\t");
                } else {
                    sb.append(DurationFormatUtils.formatDurationHMS(js.getMaxReduceTime())).append("\t")
                        .append(DurationFormatUtils.formatDurationHMS(js.getMinReduceTime())).append("\t")
                        .append(DurationFormatUtils.formatDurationHMS(js.getAvgREduceTime())).append("\t");
                }
                sb.append(js.getAlias()).append("\t")
                    .append(js.getFeature()).append("\t");
            }
            for (OutputStats os : js.getOutputs()) {
                sb.append(os.getLocation()).append(",");
            }
            sb.append("\n");
        }

        log.info(sb.toString());

        dumpDotPlan();
    }

    public void dumpDotPlan() {
        String outputFilename = new StringBuilder()
            .append(Main.getJobGraphDirectory())
            .append(getScriptName()).append('/')
            .append("jobstats-")
            .append(getDatasetIdentifier().replace('/', '-')).append('-')
            .append(getInputSize())
            .append(".dot").toString();
        File dotFile = new File(outputFilename);
        dotFile.getParentFile().mkdirs();
        try {
            new DotPlanDumper(pigStats.getJobGraph(), new PrintStream(dotFile)).dump();
            GraphvizUtil.convertToImage("png", dotFile);
        } catch (IOException e) {
            log.error("Cannot output job graph as dot graph.", e);
        }
    }
}

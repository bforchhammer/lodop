package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import de.uni_potsdam.hpi.loddp.benchmark.execution.PigScript;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;

import java.util.List;

/**
 * Contains information about the execution/run of a pig script on a specific set of input data.
 */
public class ExecutionStats {

    protected static final Log log = LogFactory.getLog(ExecutionStats.class);
    private String inputFilename;
    private PigStats pigStats;
    private PigScript pigScript;

    /**
     * Constructor.
     *
     * @param inputFilename
     * @param pigStats
     * @param pigScript
     */
    public ExecutionStats(String inputFilename, PigStats pigStats, PigScript pigScript) {
        this.inputFilename = inputFilename;
        this.pigStats = pigStats;
        this.pigScript = pigScript;
    }

    public void printStats() {
        StringBuffer sb = new StringBuffer();

        sb.append("Statistics for ").append(pigScript.getScriptName().toUpperCase()).append("\n");

        InputStats is = pigStats.getInputStats().get(0);
        sb.append("Input: ").append(is.getNumberRecords()).append(" records")
            .append(" (").append(is.getBytes()).append(" Bytes)")
            .append(" from ").append(is.getName()).append("\n");

        sb.append("Total job time = ").append(DurationFormatUtils.formatDurationHMS(pigStats.getDuration())).append("\n");

        if (pigStats.isSuccessful()) {
            sb.append("JobId\tMaps\tReduces\tMaxMapTime\tMinMapTime\tAvgMapTime\tMaxReduceTime\t" +
                "MinReduceTime\tAvgReduceTime\tAlias\tFeature\tOutputs\n");
            List<JobStats> arr = pigStats.getJobGraph().getSuccessfulJobs();
            for (JobStats js : arr) {
                String id = (js.getJobId() == null) ? "N/A" : js.getJobId().toString();
                if (js.getState() == JobStats.JobState.FAILED) {
                    sb.append(id).append("\t")
                        .append(js.getAlias()).append("\t")
                        .append(js.getFeature()).append("\t");
                    if (js.getState() == JobStats.JobState.FAILED) {
                        sb.append("Message: ").append(js.getErrorMessage()).append("\t");
                    }
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
        } else {
            sb.append("Failed Jobs:\n");
            sb.append(JobStats.FAILURE_HEADER).append("\n");
            List<JobStats> arr = pigStats.getJobGraph().getFailedJobs();
            for (JobStats js : arr) {
                sb.append(js.toString());
            }
            sb.append("\n");
        }

        log.info(sb.toString());
    }

}

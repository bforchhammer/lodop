package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.pig.tools.pigstats.PigStatsUtil;

/**
 * Class which encapsulates statistics about one task phase or a series of consecutive task phases.
 *
 * Statistics include start time, finish time, number of bytes read/written to/from the distributed (HDFS) as well as
 * local file system.
 */
public class TaskPhaseStatistics {
    private final String label;
    private long startTime = Long.MAX_VALUE;
    private long finishTime = Long.MIN_VALUE;
    private long hdfsBytesRead = 0L;
    private long hdfsBytesWritten = 0L;
    private long localBytesRead = 0L;
    private long localBytesWritten = 0L;

    public TaskPhaseStatistics(String label) {
        this.label = label;
    }

    public TaskPhaseStatistics(String label, TaskPhaseStatistics... phases) {
        this(label);
        for (TaskPhaseStatistics phase : phases) {
            update(phase);
        }
    }

    public static TaskPhaseStatistics getTaskPhaseStatistics(String phaseLabel, TaskReport[] tasks) {
        TaskPhaseStatistics phase = new TaskPhaseStatistics(phaseLabel);
        for (TaskReport task : tasks) {
            if (task.getCurrentStatus() == TIPStatus.COMPLETE) {
                phase.updateStartTime(task.getStartTime());
                phase.updateFinishTime(task.getFinishTime());
                phase.addHdfsBytesRead(task.getCounters().findCounter(PigStatsUtil.FS_COUNTER_GROUP,
                    PigStatsUtil.HDFS_BYTES_READ).getValue());
                phase.addHdfsBytesWritten(task.getCounters().findCounter(PigStatsUtil.FS_COUNTER_GROUP,
                    PigStatsUtil.HDFS_BYTES_WRITTEN).getValue());
                phase.addLocalBytesRead(task.getCounters().findCounter(PigStatsUtil.FS_COUNTER_GROUP,
                    "FILE_BYTES_READ").getValue());
                phase.addLocalBytesWritten(task.getCounters().findCounter(PigStatsUtil.FS_COUNTER_GROUP,
                    "FILE_BYTES_WRITTEN").getValue());
            }
        }
        return phase;
    }

    /**
     * Update with data from another phase statistics object.
     *
     * @param phase
     */
    public void update(TaskPhaseStatistics phase) {
        updateStartTime(phase.getStartTime());
        updateFinishTime(phase.getFinishTime());
        addHdfsBytesRead(phase.getHdfsBytesRead());
        addHdfsBytesWritten(phase.getHdfsBytesWritten());
        addLocalBytesRead(phase.getLocalBytesRead());
        addLocalBytesWritten(phase.getLocalBytesWritten());
    }

    public String getLabel() {
        return label;
    }

    public long getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    public void addHdfsBytesRead(long value) {
        this.hdfsBytesRead += value;
    }

    public void addHdfsBytesWritten(long value) {
        this.hdfsBytesWritten += value;
    }

    public long getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    public long getLocalBytesRead() {
        return localBytesRead;
    }

    public void addLocalBytesRead(long value) {
        this.localBytesRead += value;
    }

    public void addLocalBytesWritten(long value) {
        this.localBytesWritten += value;
    }

    public long getLocalBytesWritten() {
        return localBytesWritten;
    }

    public void updateStartTime(long value) {
        if (startTime > value) startTime = value;
    }

    public void updateFinishTime(long value) {
        if (finishTime < value) finishTime = value;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public long getDuration() {
        return finishTime - startTime;
    }
}

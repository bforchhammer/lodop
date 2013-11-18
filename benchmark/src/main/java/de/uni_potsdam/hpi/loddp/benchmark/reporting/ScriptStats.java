package de.uni_potsdam.hpi.loddp.benchmark.reporting;


import org.apache.pig.tools.pigstats.JobStats;

import java.util.List;

public interface ScriptStats {
    /**
     * The name of the script.
     */
    public String getScriptName();

    /**
     * The name of the data set.
     */
    public String getDatasetIdentifier();

    /**
     * The size of input data set.
     */
    public long getInputSize();

    /**
     * The size of the output data set.
     */
    public long getOutputSize();

    /**
     * The total amount of time taken for executing the script.
     */
    public long getTimeTotal();

    /**
     * The total amount of time taken up by Apache Pig.
     */
    public long getTimePig();

    /**
     * The total amount of time taken up by Hadoop.
     */
    public long getTimeMapReduce();

    /**
     * The total amount of time taken up by map reduce job setup.
     */
    public long getTimeMapReduceJobSetup();

    /**
     * The total amount of time taken up by map reduce job cleanup.
     */
    public long getTimeMapReduceJobCleanup();

    /**
     * The total amount of time taken up by map phase execution.
     */
    public long getTimeMap();

    /**
     * The total amount of time taken up by reduce phase execution.
     */
    public long getTimeReduce();

    /**
     * The number of map reduce jobs needed for the script.
     */
    public int getNumberJobs();

    public List<JobStats> getJobStats();

    /**
     * The total number of map phases.
     */
    public int getNumberMapsTotal();

    /**
     * The total number of reduce phases.
     */
    public int getNumberReducesTotal();
}

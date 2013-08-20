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

    /**
     * The average time needed for executing one map.
     */
    public long getAvgMapTimeTotal();

    /**
     * The average time needed for executing one reduce.
     */
    public long getAvgReduceTimeTotal();

}

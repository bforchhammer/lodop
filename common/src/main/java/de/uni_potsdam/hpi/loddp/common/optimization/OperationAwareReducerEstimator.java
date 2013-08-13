package de.uni_potsdam.hpi.loddp.common.optimization;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.util.UriUtil;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;
import java.util.List;

/**
 * Tries to estimate the number of reducers based on the number of output records of the corresponding map phase and the
 * type of operation.
 */
public class OperationAwareReducerEstimator implements PigReducerEstimator {
    private static final Log log = LogFactory.getLog(OperationAwareReducerEstimator.class);

    /**
     * Get the input size for as many inputs as possible. Inputs that do not report their size nor can pig look that up
     * itself are excluded from this size.
     */
    private static long getTotalInputFileSize(Configuration conf, List<POLoad> lds, Job job) throws IOException {
        long totalInputFileSize = 0;
        boolean foundSize = false;
        for (POLoad ld : lds) {
            long size = getInputSizeFromLoader(ld, job);
            if (size > -1) {
                foundSize = true;
            }
            if (size > 0) {
                totalInputFileSize += size;
                continue;
            }
            // the input file location might be a list of comma separated files,
            // separate them out
            for (String location : LoadFunc.getPathStrings(ld.getLFile().getFileName())) {
                if (UriUtil.isHDFSFileOrLocalOrS3N(location)) {
                    Path path = new Path(location);
                    FileSystem fs = path.getFileSystem(conf);
                    FileStatus[] status = fs.globStatus(path);
                    if (status != null) {
                        for (FileStatus s : status) {
                            totalInputFileSize += Utils.getPathLength(fs, s);
                            foundSize = true;
                        }
                    }
                }
            }
        }
        return foundSize ? totalInputFileSize : -1;
    }

    /**
     * Get the total input size in bytes by looking at statistics provided by loaders that implement @{link
     * LoadMetadata}.
     *
     * @param ld
     * @param job
     *
     * @return total input size in bytes, or -1 if unknown or incomplete
     *
     * @throws IOException on error
     */
    private static long getInputSizeFromLoader(POLoad ld, Job job) throws IOException {
        if (ld.getLoadFunc() == null
            || !(ld.getLoadFunc() instanceof LoadMetadata)
            || ld.getLFile() == null
            || ld.getLFile().getFileName() == null) {
            return -1;
        }

        ResourceStatistics statistics;
        try {
            statistics = ((LoadMetadata) ld.getLoadFunc())
                .getStatistics(ld.getLFile().getFileName(), job);
        } catch (Exception e) {
            log.warn("Couldn't get statistics from LoadFunc: " + ld.getLoadFunc(), e);
            return -1;
        }

        if (statistics == null || statistics.getSizeInBytes() == null) {
            return -1;
        }

        return statistics.getSizeInBytes();
    }

    @Override
    public int estimateNumberOfReducers(Job job, MapReduceOper mapReduceOper) throws IOException {
        Configuration conf = job.getConfiguration();

        long bytesPerReducer = conf.getLong(BYTES_PER_REDUCER_PARAM, DEFAULT_BYTES_PER_REDUCER);
        int maxReducers = conf.getInt(MAX_REDUCER_COUNT_PARAM, DEFAULT_MAX_REDUCER_COUNT_PARAM);

        List<POLoad> poLoads = PlanHelper.getPhysicalOperators(mapReduceOper.mapPlan, POLoad.class);
        long totalInputFileSize = getTotalInputFileSize(conf, poLoads, job);

        log.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
            + maxReducers + " totalInputFileSize=" + totalInputFileSize);

        // if totalInputFileSize == -1, we couldn't get the input size so we can't estimate.
        if (totalInputFileSize == -1) {
            return -1;
        }

        // If we have more than one flattening in a foreach, we assume that we're computing e.g. a cross product
        // and therefore want less data for each producer to work through. Probably not a very good assumption,
        // but let's see where it gets us...
        List<POForEach> poForeaches = PlanHelper.getPhysicalOperators(mapReduceOper.reducePlan, POForEach.class);
        for (POForEach poForeach : poForeaches) {
            List<Boolean> flattens = poForeach.getToBeFlattened();
            int flattenCount = 0;
            for (Boolean flatten : flattens) {
                flattenCount += flatten.booleanValue() ? 1 : 0;
            }
            if (flattenCount > 1) bytesPerReducer /= 1024;
        }

        // Compute number of reducers based on input size (same as the InputSizeEstimator).
        int reducers = (int) Math.ceil((double) totalInputFileSize / bytesPerReducer);
        reducers = Math.max(1, reducers);
        reducers = Math.min(maxReducers, reducers);
        return reducers;
    }

}

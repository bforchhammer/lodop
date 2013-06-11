package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import de.uni_potsdam.hpi.loddp.benchmark.execution.InputFileSet;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.pig.tools.pigstats.JobStats;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

public class ReportGenerator {
    protected static final Log log = LogFactory.getLog(ReportGenerator.class);
    private static final NumberFormat DECIMAL_SCIENTIFIC = new DecimalFormat("0.#E0");
    private static final NumberFormat DECIMAL_FLOAT = new DecimalFormat("#.#");
    private Map<String, Map<String, Map<Long, ExecutionStats>>> statsByNameAndSize = new TreeMap<String, Map<String,
        Map<Long, ExecutionStats>>>();
    private Map<String, Map<Long, Map<String, ExecutionStats>>> statsBySizeAndName = new TreeMap<String, Map<Long,
        Map<String, ExecutionStats>>>();

    /**
     * Constructor.
     *
     * @param stats
     */
    public ReportGenerator(Set<ExecutionStats> stats) {
        init(stats);
    }

    /**
     * Parses the set of given execution stats and stores them in grouped maps.
     *
     * @param stats A set of execution stats.
     */
    private void init(Set<ExecutionStats> stats) {
        for (ExecutionStats stat : stats) {
            String dataset = stat.getDatasetIdentifier();
            if (!statsBySizeAndName.containsKey(dataset)) {
                statsBySizeAndName.put(dataset, new TreeMap<Long, Map<String, ExecutionStats>>());
            }
            if (!statsByNameAndSize.containsKey(dataset)) {
                statsByNameAndSize.put(dataset, new TreeMap<String, Map<Long, ExecutionStats>>());
            }

            String name = stat.getScriptName();
            long size = stat.getInputSize();

            Map<String, ExecutionStats> statsByName;
            if (!statsBySizeAndName.get(dataset).containsKey(size)) {
                statsByName = new HashMap<String, ExecutionStats>();
                statsBySizeAndName.get(dataset).put(size, statsByName);
            } else {
                statsByName = statsBySizeAndName.get(dataset).get(size);
            }

            statsByName.put(name, stat);

            Map<Long, ExecutionStats> statsBySize;
            if (!statsByNameAndSize.get(dataset).containsKey(name)) {
                statsBySize = new HashMap<Long, ExecutionStats>();
                statsByNameAndSize.get(dataset).put(name, statsBySize);
            } else {
                statsBySize = statsByNameAndSize.get(dataset).get(name);
            }

            statsBySize.put(size, stat);
        }
    }

    public void scalabilityReport() {
        for (String dataset : statsByNameAndSize.keySet()) {
            scalabilityReport(dataset);
        }
    }

    public void scalabilityReport(String dataset) {
        scalabilityReport(dataset, ReportType.EXECUTION_TIME);
        scalabilityReport(dataset, ReportType.OUTPUT_SIZE);
    }

    public void scalabilityReport(InputFileSet dataset) {
        scalabilityReport(dataset.getIdentifier());
    }

    public void scalabilityReport(InputFileSet dataset, ReportType type) {
        scalabilityReport(dataset.getIdentifier(), type);
    }

    public void scalabilityReport(String dataset, ReportType type) {
        StringBuilder sb = new StringBuilder();

        sb.append("Scalability (");
        sb.append(type);
        sb.append("):\n");

        // Build table header:
        sb.append("Script name");
        for (Long size : statsBySizeAndName.get(dataset).keySet()) {
            sb.append("\t");
            sb.append(DECIMAL_SCIENTIFIC.format(size));
        }
        sb.append("\n");

        for (String scriptName : statsByNameAndSize.get(dataset).keySet()) {
            sb.append(scriptName);
            Map<Long, ExecutionStats> statsBySize = statsByNameAndSize.get(dataset).get(scriptName);
            for (Long size : statsBySizeAndName.get(dataset).keySet()) {
                sb.append("\t");
                if (statsBySize.containsKey(size)) {
                    ExecutionStats stat = statsBySize.get(size);
                    switch (type) {
                        case EXECUTION_TIME:
                            sb.append(DurationFormatUtils.formatDurationHMS(stat.getTimeTotal()));
                            break;
                        case OUTPUT_SIZE:
                            sb.append(stat.getOutputSize());
                            break;
                    }
                }
            }
            sb.append("\n");
        }
        log.info(sb.toString());
    }

    public void scriptComparison() {
        for (String dataset : statsByNameAndSize.keySet()) {
            scriptComparison(dataset);
        }
    }

    public void scriptComparison(InputFileSet dataset) {
        scriptComparison(dataset.getIdentifier());
    }

    public void scriptComparison(String dataset) {
        Long maxInputSize = Collections.max(statsBySizeAndName.get(dataset).keySet());
        scriptComparison(dataset, maxInputSize);
    }

    public void scriptComparison(InputFileSet dataset, Long inputSize) {
        scriptComparison(dataset.getIdentifier(), inputSize);
    }

    public void scriptComparison(String dataset, Long inputSize) {
        StringBuilder sb = new StringBuilder();
        sb.append("Comparison of script counters (");
        sb.append(DECIMAL_SCIENTIFIC.format(inputSize));
        sb.append(" records)");
        sb.append("\n");

        sb.append("Script name");
        sb.append("\t").append("Time (total)");
        sb.append("\t").append("Number of jobs");
        sb.append("\t").append("Number of maps");
        sb.append("\t").append("Average map time");
        sb.append("\t").append("Number of reduces");
        sb.append("\t").append("Average reduce time");
        sb.append("\n");

        Map<String, ExecutionStats> statsByName = statsBySizeAndName.get(dataset).get(inputSize);
        for (ExecutionStats stat : statsByName.values()) {
            sb.append(stat.getScriptName());
            sb.append("\t").append(DurationFormatUtils.formatDurationHMS(stat.getTimeTotal()));
            sb.append("\t").append(stat.getNumberJobs());
            sb.append("\t").append(stat.getNumberMapsTotal());
            sb.append("\t").append(DurationFormatUtils.formatDurationHMS(stat.getAvgMapTimeTotal()));
            sb.append("\t").append(stat.getNumberReducesTotal());
            sb.append("\t").append(DurationFormatUtils.formatDurationHMS(stat.getAvgReduceTimeTotal()));
            sb.append("\n");
        }

        log.info(sb.toString());
    }

    public void datasetComparison(InputFileSet dataset1, InputFileSet dataset2, long inputSize) {
        datasetComparison(dataset1.getIdentifier(), dataset2.getIdentifier(), inputSize);
    }

    public void datasetComparison(String dataset1, String dataset2, long inputSize) {
        if (dataset1 == null || dataset1.isEmpty() || dataset2 == null || dataset2.isEmpty()) {
            log.error("Cannot compare datasets, because one of the identifiers is empty.");
            return;
        }
        if (!statsBySizeAndName.containsKey(dataset1) || !statsBySizeAndName.get(dataset1).containsKey(inputSize)) {
            log.error(String.format("Cannot compare datasets %s and %s, because there are no suitable results for %s.",
                dataset1, dataset2, dataset1));
        }
        if (!statsBySizeAndName.containsKey(dataset2) || !statsBySizeAndName.get(dataset2).containsKey(inputSize)) {
            log.error(String.format("Cannot compare datasets %s and %s, because there are no suitable results for %s.",
                dataset1, dataset2, dataset2));
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Comparison of ").append(dataset1).append(" and ").append(dataset2)
            .append(" for ").append(DECIMAL_SCIENTIFIC.format(inputSize)).append(" tuple.\n");

        // @todo implement comparison

        log.info(sb.toString());

        throw new NotImplementedException();
    }

    public void featureRuntimeAnalysis() {
        for (String dataset : statsByNameAndSize.keySet()) {
            featureRuntimeAnalysis(dataset);
        }
    }

    public void featureRuntimeAnalysis(String dataset) {
        Long maxInputSize = Collections.max(statsBySizeAndName.get(dataset).keySet());
        featureRuntimeAnalysis(dataset, maxInputSize);
    }

    /**
     * Tries to highlight runtime implications of different features used by pig scripts.
     */
    public void featureRuntimeAnalysis(String dataset, long inputSize) {
        Map<String, ExecutionStats> statsByName = statsBySizeAndName.get(dataset).get(inputSize);
        Map<String, Map<String, SummaryStatistics>> statsByFeatures = new TreeMap<String, Map<String, SummaryStatistics>>();

        final String[] counterTypes = new String[] {
            "mapMaxTime", "mapMinTime", "mapNumber", "mapInputNumber", "mapOutputNumber",
            "reduceMaxTime", "reduceMinTime", "reduceNumber", "reduceInputNumber", "reduceOutputNumber",
        };
        final String[] timeCounterTypes = new String[] {
            "mapMaxTime", "mapMinTime", "reduceMaxTime", "reduceMinTime",
        };

        for (ExecutionStats stats : statsByName.values()) {
            List<JobStats> jobs = stats.getJobStats();
            for (JobStats js : jobs) {
                if (!statsByFeatures.containsKey(js.getFeature())) {
                    statsByFeatures.put(js.getFeature(), new HashMap<String, SummaryStatistics>());
                }
                Map<String, SummaryStatistics> counters = statsByFeatures.get(js.getFeature());

                for (String counterType : counterTypes) {
                    if (!counters.containsKey(counterType)) {
                        counters.put(counterType, new SummaryStatistics());
                    }
                }

                counters.get("mapMaxTime").addValue(js.getMaxMapTime());
                counters.get("mapMinTime").addValue(js.getMinMapTime());
                counters.get("mapNumber").addValue(js.getNumberMaps());
                counters.get("mapInputNumber").addValue(js.getMapInputRecords());
                counters.get("mapOutputNumber").addValue(js.getMapOutputRecords());
                counters.get("reduceMaxTime").addValue(js.getMaxReduceTime());
                counters.get("reduceMinTime").addValue(js.getMinReduceTime());
                counters.get("reduceNumber").addValue(js.getNumberReduces());
                counters.get("reduceInputNumber").addValue(js.getReduceInputRecords());
                counters.get("reduceOutputNumber").addValue(js.getReduceOutputRecords());
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Comparison of PIG features.\n");
        sb.append("Feature");
        sb.append("\t").append("Usage count");
        for (String counterType : counterTypes) {
            sb.append("\t").append(counterType).append(" MEAN");
            sb.append("\t").append(counterType).append(" STD");
        }
        sb.append("\n");

        for (String feature : statsByFeatures.keySet()) {
            sb.append(feature);
            sb.append("\t").append(statsByFeatures.get(feature).get("mapMaxTime").getN());
            for (String counterType : counterTypes) {
                SummaryStatistics s = statsByFeatures.get(feature).get(counterType);
                if (ArrayUtils.contains(timeCounterTypes, counterType)) {
                    sb.append("\t").append(DurationFormatUtils.formatDurationHMS(Math.round(s.getMean())));
                    sb.append("\t").append(DECIMAL_FLOAT.format(s.getStandardDeviation() / 1000)).append("s");
                }
                else {
                    sb.append("\t").append(DECIMAL_FLOAT.format(s.getMean()));
                    sb.append("\t").append(DECIMAL_FLOAT.format(s.getStandardDeviation())).append("s");
                }
            }
            sb.append("\n");
        }

        log.info(sb.toString());
    }

    public static enum ReportType {
        EXECUTION_TIME("Execution time"),
        OUTPUT_SIZE("Number of output records");
        private String name;

        ReportType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

}

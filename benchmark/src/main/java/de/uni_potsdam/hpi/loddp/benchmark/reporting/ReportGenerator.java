package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

public class ReportGenerator {
    protected static final Log log = LogFactory.getLog(ReportGenerator.class);
    private static final NumberFormat DECIMAL_SCIENTIFIC = new DecimalFormat("0.#E0");
    private Map<String, Map<Long, ExecutionStats>> statsByNameAndSize = new TreeMap<String, Map<Long, ExecutionStats>>();
    private Map<Long, Map<String, ExecutionStats>> statsBySizeAndName = new TreeMap<Long, Map<String, ExecutionStats>>();

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
            String name = stat.getScriptName();
            long size = stat.getInputSize();

            Map<String, ExecutionStats> statsByName;
            if (!statsBySizeAndName.containsKey(size)) {
                statsByName = new HashMap<String, ExecutionStats>();
                statsBySizeAndName.put(size, statsByName);
            } else {
                statsByName = statsBySizeAndName.get(size);
            }

            statsByName.put(name, stat);

            Map<Long, ExecutionStats> statsBySize;
            if (!statsByNameAndSize.containsKey(name)) {
                statsBySize = new HashMap<Long, ExecutionStats>();
                statsByNameAndSize.put(name, statsBySize);
            } else {
                statsBySize = statsByNameAndSize.get(name);
            }

            statsBySize.put(size, stat);
        }
    }

    public void scalabilityReport() {
        scalabilityReport(ReportType.EXECUTION_TIME);
        scalabilityReport(ReportType.OUTPUT_SIZE);
    }

    public void scalabilityReport(ReportType type) {
        StringBuilder sb = new StringBuilder();

        sb.append("Scalability (");
        sb.append(type);
        sb.append("):\n");

        // Build table header:
        sb.append("Script name");
        for (Long size : statsBySizeAndName.keySet()) {
            sb.append("\t");
            sb.append(DECIMAL_SCIENTIFIC.format(size));
        }
        sb.append("\n");

        for (String scriptName : statsByNameAndSize.keySet()) {
            sb.append(scriptName);
            Map<Long, ExecutionStats> statsBySize = statsByNameAndSize.get(scriptName);
            for (Long size : statsBySizeAndName.keySet()) {
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
        Long maxInputSize = Collections.max(statsBySizeAndName.keySet());
        scriptComparison(maxInputSize);
    }

    protected void scriptComparison(Long inputSize) {
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

        Map<String, ExecutionStats> statsByName = statsBySizeAndName.get(inputSize);
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

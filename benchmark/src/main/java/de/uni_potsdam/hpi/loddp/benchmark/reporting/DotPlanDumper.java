package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.JobStats;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * DOT plan dumper for a map-reduce statistics plan (Operators are JobStat objects).
 */
public class DotPlanDumper extends org.apache.pig.newplan.DotPlanDumper {

    protected static final Log log = LogFactory.getLog(DotPlanDumper.class);
    private static final NumberFormat DECIMAL_FLOAT = new DecimalFormat("#.#");

    public DotPlanDumper(BaseOperatorPlan plan, PrintStream ps) {
        super(plan, ps);
    }

    @Override
    protected String getName(Operator op) {
        if (op instanceof JobStats) {
            JobStats stats = (JobStats) op;

            if (stats.getHadoopCounters() == null) {
                log.warn("No Hadoop Counters found; did the Job crash?");
                return super.getName(op);
            }

            Counters.Group hadoopStats = stats.getHadoopCounters().getGroup("org.apache.hadoop.mapred.Task$Counter");
            Counters.Counter combineInputRecords = hadoopStats.getCounterForName("COMBINE_INPUT_RECORDS");
            Counters.Counter combineOutputRecords = hadoopStats.getCounterForName("COMBINE_OUTPUT_RECORDS");
            Counters.Counter reduceInputGroups = hadoopStats.getCounterForName("REDUCE_INPUT_GROUPS");

            double recordsPerGroup = -1;
            if (reduceInputGroups.getCounter() != 0) {
                recordsPerGroup = stats.getReduceInputRecords() / reduceInputGroups.getCounter();
            }

            StringBuilder sb = new StringBuilder();
            sb.append(stats.getJobId());
            sb.append("\\l").append("Alias:   ").append(stats.getAlias());
            sb.append("\\l").append("Feature: ").append(stats.getFeature());
            sb.append("\\l").append("Map:     ")
                .append(stats.getMapInputRecords()).append(" --> ").append(stats.getMapOutputRecords())
                .append(" | ").append(stats.getNumberMaps()).append("x")
                .append(" | ").append(stats.getAvgMapTime()).append("ms (avg)");

            if (combineInputRecords.getCounter() > 0) {
                sb.append("\\l").append("Combine: ")
                    .append(combineInputRecords.getCounter()).append(" --> ").append(combineOutputRecords.getCounter());
            }
            sb.append("\\l").append("Reduce:  ")
                .append(stats.getReduceInputRecords()).append(" --> ").append(stats.getReduceOutputRecords())
                .append(" | ").append(stats.getNumberReduces()).append("x")
                .append(" | ").append(stats.getAvgREduceTime()).append("ms (avg)");

            sb.append("\\l").append("         ").append(DECIMAL_FLOAT.format(recordsPerGroup))
                .append(" records/group (avg) in ").append(reduceInputGroups.getCounter()).append(" groups ");

            return sb.toString();
        }
        return super.getName(op);
    }
}

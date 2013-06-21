package de.uni_potsdam.hpi.loddp.benchmark.reporting;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.JobStats;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class DotPlanDumper extends org.apache.pig.newplan.DotPlanDumper {

    protected static final Log log = LogFactory.getLog(DotPlanDumper.class);
    private static final NumberFormat DECIMAL_FLOAT = new DecimalFormat("#.#");
    private File imageOutputFile;
    private File dotOutputFile;
    private String imageType;

    private DotPlanDumper(BaseOperatorPlan plan, File imageOutputFile,
                          File dotOutputFile, String imageType) throws FileNotFoundException {
        super(plan, new PrintStream(dotOutputFile));
        this.imageOutputFile = imageOutputFile;
        this.dotOutputFile = dotOutputFile;
        this.imageType = imageType;
    }

    public static DotPlanDumper createInstance(BaseOperatorPlan plan, String imageOutputFilename) throws IOException {
        String imageType = FilenameUtils.getExtension(imageOutputFilename);
        return createInstance(plan, imageOutputFilename, imageType);
    }

    public static DotPlanDumper createInstance(BaseOperatorPlan plan, String imageOutputFilename, String imageType) throws IOException {
        String dotOutputFilename = FilenameUtils.removeExtension(imageOutputFilename) + ".dot";
        return createInstance(plan, imageOutputFilename, dotOutputFilename, imageType);
    }

    public static DotPlanDumper createInstance(BaseOperatorPlan plan, String imageOutputFilename,
                                               String dotOutputFilename, String imageType) throws IOException {
        File dotOutputFile = new File(dotOutputFilename);
        File imageOutputFile = new File(imageOutputFilename);
        dotOutputFile.getParentFile().mkdirs();
        dotOutputFile.createNewFile();
        return new DotPlanDumper(plan, imageOutputFile, dotOutputFile, imageType);
    }

    public void dumpAsImage() throws IOException {
        dump();

        try {
            Runtime rt = Runtime.getRuntime();
            String[] args = {"/usr/bin/dot",
                "-T" + imageType,
                dotOutputFile.getAbsolutePath(),
                "-o", imageOutputFile.getAbsolutePath()};
            Process p = rt.exec(args);
            p.waitFor();
        } catch (InterruptedException e) {
            log.error("Cannot output job graph as png graph.", e);
        }
    }

    @Override
    protected String getName(Operator op) {
        if (op instanceof JobStats) {
            JobStats stats = (JobStats) op;

            Counters.Group hadoopStats = stats.getHadoopCounters().getGroup("org.apache.hadoop.mapred.Task$Counter");
            Counters.Counter combineInputRecords = hadoopStats.getCounterForName("COMBINE_INPUT_RECORDS");
            Counters.Counter combineOutputRecords = hadoopStats.getCounterForName("COMBINE_OUTPUT_RECORDS");
            Counters.Counter reduceInputGroups = hadoopStats.getCounterForName("REDUCE_INPUT_GROUPS");

            double recordsPerGroup = stats.getReduceInputRecords() / reduceInputGroups.getCounter();

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

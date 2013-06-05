package de.uni_potsdam.hpi.loddp.benchmark;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.joda.time.DateTime;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Main class.
 */
public class Main {

    protected static final Log log;
    protected static final String LOG_DIRECTORY;
    protected static final String LOG_FILENAME_APACHE = "apache.log";
    protected static final String LOG_FILENAME_BENCHMARK = "benchmark.log";

    static {
        // Setup directory for log files (this will only work, if executed before any logger is initialised.
        LOG_DIRECTORY = String.format("logs/%s", new DateTime().toString("YYYY-MM-dd-HH-mm-ss"));
        new File(LOG_DIRECTORY).mkdirs();
        System.setProperty("log.directory", LOG_DIRECTORY);
        System.out.println("Logging to = " + System.getProperty("log.directory"));

        // Configure file names for log files.
        System.setProperty("log.filename.apache", LOG_FILENAME_APACHE);
        System.setProperty("log.filename.benchmark", LOG_FILENAME_BENCHMARK);

        // Initialize Logger.
        log = LogFactory.getLog(Main.class);
    }

    /**
     * Looks for scripts, and runs complete benchmark.
     *
     * @param args
     */
    public static void main(String[] args) {
        Set<PigScript> scripts = PigScriptHelper.findPigScripts();

        Set<String> blacklist = new HashSet<String>();
        /*blacklist.add("classes_by_entity");
        blacklist.add("classes_by_url");
        blacklist.add("classes_by_tld");
        blacklist.add("classes_by_pld");
        blacklist.add("incoming_property_cooc");
        blacklist.add("number_of_triples");
        blacklist.add("property_cooc_by_entities");
        blacklist.add("property_cooc_by_urls");
        blacklist.add("properties_by_entity");
        blacklist.add("properties_by_pld");
        blacklist.add("properties_by_statement");
        blacklist.add("properties_by_tld");
        blacklist.add("properties_by_url");
        blacklist.add("vocabularies_by_entity");
        blacklist.add("vocabularies_by_pld");
        blacklist.add("vocabularies_by_tld");
        blacklist.add("vocabularies_by_url");*/

        boolean reuseServer = false;
        ScriptRunner runner = new ScriptRunner(reuseServer);
        runSequential(runner, scripts, blacklist);
    }

    /**
     * Benchmark the given set of pig scripts.
     *
     * @param runner  A script runner.
     * @param scripts A set of pig scripts.
     */
    public static void runSequential(ScriptRunner runner, Set<PigScript> scripts) {
        runSequential(runner, scripts, new HashSet<String>());
    }

    /**
     * Benchmark the given set of pig scripts.
     *
     * @param runner    A script runner.
     * @param scripts   A set of pig scripts.
     * @param blacklist A list of script names to skip and not execute.
     */
    public static void runSequential(ScriptRunner runner, Set<PigScript> scripts, Set<String> blacklist) {
        for (PigScript script : scripts) {
            StringBuilder sb = new StringBuilder();
            sb.append(script);
            if (blacklist.contains(script.getScriptName())) {
                sb.append(" - SKIPPED");
                log.info(sb.toString());
            } else {
                sb.append(" - RUNNING");
                log.info(sb.toString());
                runScript(runner, script);
            }
        }
    }

    /**
     * Execute the given pig script.
     *
     * @param runner A script runner.
     * @param script A pig script.
     */
    private static void runScript(ScriptRunner runner, PigScript script) {
        PigStats stats = runner.runScript(script);
        if (stats != null) {
            printStats(script.getScriptName(), stats);
        }
    }

    private static void printStats(String scriptName, PigStats stats) {
        StringBuffer sb = new StringBuffer();

        sb.append("Total job time = ");
        sb.append(DurationFormatUtils.formatDurationHMS(stats.getDuration()));
        sb.append("\n");

        if (stats.isSuccessful()) {
            sb.append("JobId\tMaps\tReduces\tMaxMapTime\tMinMapTime\tAvgMapTime\tMaxReduceTime\t" +
                "MinReduceTime\tAvgReduceTime\tAlias\tFeature\tOutputs\n");
            List<JobStats> arr = stats.getJobGraph().getSuccessfulJobs();
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
            List<JobStats> arr = stats.getJobGraph().getFailedJobs();
            for (JobStats js : arr) {
                sb.append(js.toString());
            }
            sb.append("\n");
        }

        log.info(sb.toString());
    }
}

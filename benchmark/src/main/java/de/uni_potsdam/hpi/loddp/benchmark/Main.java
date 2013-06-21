package de.uni_potsdam.hpi.loddp.benchmark;

import de.uni_potsdam.hpi.loddp.benchmark.execution.*;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ExecutionStats;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ReportGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.tools.pigstats.PigStats;
import org.joda.time.DateTime;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * Main class.
 */
public class Main {

    protected static final Log log;
    protected static final String LOG_DIRECTORY;
    protected static final String LOG_FILENAME_APACHE = "apache.log";
    protected static final String LOG_FILENAME_BENCHMARK = "benchmark.log";
    protected static final String LOG_FILENAME_REPORTING = "report.log";
    protected static final String JOB_GRAPH_DIRECTORY = "jobs";
    protected static final String HDFS_WORKING_DIRECTORY = "";

    static {
        // Setup directory for log files (this will only work, if executed before any logger is initialised.
        LOG_DIRECTORY = String.format("logs/%s", new DateTime().toString("YYYY-MM-dd-HH-mm-ss"));
        new File(LOG_DIRECTORY).mkdirs();
        System.setProperty("log.directory", LOG_DIRECTORY);
        System.out.println("Logging to = " + System.getProperty("log.directory"));

        // Configure file names for log files.
        System.setProperty("log.filename.apache", LOG_FILENAME_APACHE);
        System.setProperty("log.filename.benchmark", LOG_FILENAME_BENCHMARK);
        System.setProperty("log.filename.reporting", LOG_FILENAME_REPORTING);

        // Initialize Logger.
        log = LogFactory.getLog(Main.class);
    }

    private static Set<ExecutionStats> statisticsCollection = new HashSet<ExecutionStats>();
    private static Set<PigScript> scripts;

    public static String getJobGraphDirectory() {
        return LOG_DIRECTORY + '/' + JOB_GRAPH_DIRECTORY + '/';
    }

    /**
     * Looks for scripts, and runs complete benchmark.
     *
     * @param args
     */
    public static void main(String[] args) {
        scripts = PigScriptHelper.findPigScripts();
        ScriptRunner runner = new ScriptRunner(ScriptRunner.HADOOP_LOCATION.LOCALHOST, HDFS_WORKING_DIRECTORY);

        run_smallTest(runner);
    }

    private static void run_smallTest(ScriptRunner runner) {
        // Execute only three scripts:
        Set<String> blacklist = PigScriptHelper.getBlackList(new String[] {"number_of_instances", "classes_by_entity",
            "classes_by_url"});

        // Use DBpedia 1M.
        InputFile inputFile = new InputFile("data/dbpedia-1M.nq.gz");

        // Execute script.
        runSequential(runner, scripts, inputFile, blacklist);

        // Generate a bunch of numbers and tables and stuff.
        ReportGenerator rg = new ReportGenerator(statisticsCollection);
        rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();
    }

    private static void run_scalabilityDBPedia(ScriptRunner runner) {
        // No cooc-scripts
        Set<String> blacklist = PigScriptHelper.getBlackList(PigScriptHelper.SCRIPT_LIST.NO_COOC);

        // First, dbpedia 10M to 1000M, then full dbpedia.
        InputFileSet dbpedia = InputFileSet.createLogarithmic("BTC2012/DBPedia", "data/dbpedia-", 1000000, 100000000);
        InputFile dbpediaAll = new InputFile("data/dbpedia-full.nq.gz");

        ReportGenerator rg = new ReportGenerator(statisticsCollection);

        runSequential(runner, scripts, dbpedia.getBySize(1000000), blacklist);
        rg.initialise();
        //rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();

        runSequential(runner, scripts, dbpedia.getBySize(10000000), blacklist);
        rg.initialise();
        //rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();

        runSequential(runner, scripts, dbpedia.getBySize(100000000), blacklist);
        rg.initialise();
        //rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();

        runSequential(runner, scripts, dbpediaAll, blacklist);
        rg.initialise();
        rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();
        //rg.datasetComparison(freebase, dbpedia, 10000000);
    }

    private static void run_scalabilityDBPedia2(ScriptRunner runner) {
        // No cooc-scripts
        Set<String> blacklist = PigScriptHelper.getBlackList(PigScriptHelper.SCRIPT_LIST.NO_COOC);

        // data/dbpedia-1M.nq.gz ... data/dbpedia-20M.nq.gz
        InputFileSet dbpedia = InputFileSet.createLinear("BTC2012/DBPedia", "data/dbpedia-", 1, 20, "M.nq.gz",
            1000000);

        ReportGenerator rg = new ReportGenerator(statisticsCollection);
        for (InputFile file : dbpedia.getAll()) {
            runSequential(runner, scripts, file, blacklist);
            rg.initialise();
            rg.scalabilityReport();
            rg.scriptComparison();
            rg.featureRuntimeAnalysis();
        }
    }

    /**
     * Benchmark the given set of pig scripts.
     *
     * @param runner    A script runner.
     * @param scripts   A set of pig scripts.
     * @param input     The quads input file.
     * @param blacklist A list of script names to skip and not execute.
     */
    public static void runSequential(ScriptRunner runner, Set<PigScript> scripts, InputFile input, Set<String> blacklist) {
        for (PigScript script : scripts) {
            StringBuilder sb = new StringBuilder();
            sb.append(script);
            if (blacklist.contains(script.getScriptName())) {
                sb.append(" - SKIPPED");
                log.info(sb.toString());
            } else {
                sb.append(" - RUNNING");
                log.info(sb.toString());
                runScript(runner, script, input);
            }
        }
    }

    /**
     * Execute the given pig script.
     *
     * @param runner A script runner.
     * @param script A pig script.
     * @param input  The quads input file.
     */
    private static void runScript(ScriptRunner runner, PigScript script, InputFile input) {
        PigStats stats = runner.runScript(script, input);
        if (stats != null) {
            ExecutionStats s = new ExecutionStats(input, stats, script);
            statisticsCollection.add(s);
            s.printStats();
        }
    }
}

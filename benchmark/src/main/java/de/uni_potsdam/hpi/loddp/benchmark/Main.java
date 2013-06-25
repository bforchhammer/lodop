package de.uni_potsdam.hpi.loddp.benchmark;

import de.uni_potsdam.hpi.loddp.benchmark.execution.InputFile;
import de.uni_potsdam.hpi.loddp.benchmark.execution.PigScript;
import de.uni_potsdam.hpi.loddp.benchmark.execution.PigScriptHelper;
import de.uni_potsdam.hpi.loddp.benchmark.execution.ScriptRunner;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ExecutionStats;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ReportGenerator;
import org.apache.commons.cli.*;
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

    private static Options getCliOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message.");
        options.addOption(OptionBuilder
            .withLongOpt("scripts")
            .withDescription("Comma-separated list of pig script names to execute.")
            .hasArgs()
            .withArgName("number_of_instances")
            .withValueSeparator(',')
            .create('s'));
        options.addOption(OptionBuilder
            .withLongOpt("cluster")
            .withDescription("Use tenemhead2 cluster for computation.")
            .hasArg(false)
            .create('c'));

        return options;
    }

    /**
     * Looks for scripts, and runs complete benchmark.
     *
     * @param args
     */
    public static void main(String[] args) {
        Options options = getCliOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            new HelpFormatter().printHelp("./gradlew run", options);
            return;
        }

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp("./gradlew run", options);
            return;
        }

        // Determine hadoop location, by default use localhost.
        ScriptRunner.HADOOP_LOCATION hadoopLocation = ScriptRunner.HADOOP_LOCATION.LOCALHOST;
        if (cmd.hasOption("cluster")) {
            hadoopLocation = ScriptRunner.HADOOP_LOCATION.HPI_CLUSTER;
        }

        // By default execute all scripts.
        Set<PigScript> scripts = null;
        if (cmd.hasOption("scripts")) {
            scripts = PigScriptHelper.findPigScripts(cmd.getOptionValues('s'));
        } else {
            scripts = PigScriptHelper.findPigScripts();
        }

        // Use DBpedia 1M.
        InputFile inputFile = new InputFile("data/dbpedia-1M.nq.gz");
        ScriptRunner runner = new ScriptRunner(hadoopLocation, HDFS_WORKING_DIRECTORY);
        runScripts(runner, scripts, inputFile);

        // Generate a bunch of numbers and tables and stuff.
        ReportGenerator rg = new ReportGenerator(statisticsCollection);
        rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();
    }

    /**
     * Benchmark the given set of pig scripts.
     *
     * @param runner  A script runner.
     * @param scripts A set of pig scripts.
     * @param input   The quads input file.
     */
    public static void runScripts(ScriptRunner runner, Set<PigScript> scripts, InputFile input) {
        for (PigScript script : scripts) {
            StringBuilder sb = new StringBuilder();
            sb.append(script);
            sb.append(" - RUNNING");
            log.info(sb.toString());
            runScript(runner, script, input);
        }
    }

    /**
     * Execute the given pig script.
     *
     * @param runner A script runner.
     * @param script A pig script.
     * @param input  The quads input file.
     */
    public static void runScript(ScriptRunner runner, PigScript script, InputFile input) {
        PigStats stats = runner.runScript(script, input);
        if (stats != null) {
            ExecutionStats s = new ExecutionStats(input, stats, script);
            statisticsCollection.add(s);
            s.printStats();
        }
    }
}

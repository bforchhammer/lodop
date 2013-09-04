package de.uni_potsdam.hpi.loddp.benchmark;

import de.uni_potsdam.hpi.loddp.benchmark.execution.InputFile;
import de.uni_potsdam.hpi.loddp.benchmark.execution.ScriptRunner;
import de.uni_potsdam.hpi.loddp.benchmark.execution.ScriptRunnerFactory;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ReportGenerator;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.HadoopLocation;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScriptFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import java.io.File;
import java.util.*;

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
    protected static final String HDFS_DATA_DIRECTORY = "data";

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

    private static Set<ScriptStats> statisticsCollection = new HashSet<ScriptStats>();
    private static Set<PigScript> scripts;

    public static String getJobGraphDirectory() {
        return LOG_DIRECTORY + '/' + JOB_GRAPH_DIRECTORY + '/';
    }

    private static Options getCliOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message.");
        options.addOption(OptionBuilder
            .withLongOpt("scripts")
            .withDescription("Space-separated list of pig script names to execute. Asterisk (*) can be used as a wildcard.")
            .hasArgs()
            .withArgName("number_of_instances")
            .create('s'));
        options.addOption(OptionBuilder
            .withLongOpt("cluster")
            .withDescription("Use tenemhead2 cluster for computation.")
            .hasArg(false)
            .create('c'));
        options.addOption(OptionBuilder
            .withLongOpt("datasets")
            .withDescription("Filename of the dataset to be loaded. Dataset will be loaded from hdfs://" +
                HDFS_WORKING_DIRECTORY + HDFS_DATA_DIRECTORY + "/[dataset].nq.gz." +
                "Multiple datasets can be specified and are executed in sequence.")
            .hasArgs().withArgName("dbpedia-1M")
            .create('d')
        );
        options.addOption(OptionBuilder
            .withLongOpt("limit")
            .withDescription("Automatically limit all results sets to the given size.")
            .hasArg().withArgName("1000")
            .create('l')
        );
        options.addOption(OptionBuilder
            .withLongOpt("explain")
            .withDescription("Dumps the logical, physical and mapreduce operator plans as DOT graphs for each script.")
            .hasArg(false)
            .create('e'));
        options.addOption(OptionBuilder
            .withLongOpt("repeat")
            .withDescription("Repeats the workflow the specified number of times and computes the average runtime.")
            .hasArg().withArgName("3")
            .create('r'));
        options.addOption(OptionBuilder
            .withLongOpt("merge")
            .withDescription("Merge scripts and execute the merged plan.")
            .hasArg(false)
            .create('m'));
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
        String cmdLineSyntax = "./gradlew :benchmark:run -PappArgs=\"[args]\"";
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage());
            new HelpFormatter().printHelp(cmdLineSyntax, options);
            return;
        }

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp(cmdLineSyntax, options);
            return;
        }

        // Determine hadoop location, by default use localhost.
        HadoopLocation hadoopLocation = HadoopLocation.LOCALHOST;
        if (cmd.hasOption("cluster")) {
            hadoopLocation = HadoopLocation.HPI_CLUSTER;
        }

        // By default execute all scripts.
        Set<PigScript> scripts = null;
        if (cmd.hasOption("scripts")) {
            scripts = PigScriptFactory.findPigScripts(cmd.getOptionValues("scripts"));
        } else {
            scripts = PigScriptFactory.findPigScripts();
        }

        // Determine dataset to run.
        List<InputFile> inputFiles = new ArrayList<InputFile>();
        if (cmd.hasOption("datasets")) {
            String[] values = cmd.getOptionValues("datasets");
            for (String value : values) {
                String filename = normalizeDatasetFilename(value);
                inputFiles.add(new InputFile(filename));
            }
        } else {
            // Use DBpedia 1M.
            inputFiles.add(new InputFile(HDFS_DATA_DIRECTORY + "/dbpedia-1M.nq.gz"));
        }
        logInfo(inputFiles);

        int repeat = 1;
        if (cmd.hasOption("repeat")) {
            repeat = Integer.parseInt(cmd.getOptionValue("repeat"));
            if (repeat < 1) {
                repeat = 1;
                log.warn("--repeat parameter ignored because the specified value was negative or zero (positive " +
                    "integer expected).");
            }
            log.info(String.format("Repeating each script %d times.", repeat));
        }

        boolean mergeScripts = false;
        if (cmd.hasOption("merge")) {
            mergeScripts = true;
            log.info("Scripts are merged into one large plan.");
        }

        ScriptRunner runner = ScriptRunnerFactory.getScriptRunner(hadoopLocation, HDFS_WORKING_DIRECTORY,
            mergeScripts, repeat);

        List<ScriptStats> statistics = runner.execute(scripts, inputFiles);

        ReportGenerator rg = new ReportGenerator(statistics);
        rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();
    }

    private static String normalizeDatasetFilename(String value) {
        StringBuilder sb = new StringBuilder();
        if (FilenameUtils.getPrefixLength(value) <= 0) {
            sb.append(HDFS_DATA_DIRECTORY).append("/");
        }
        sb.append(value);
        if (FilenameUtils.indexOfExtension(value) == -1) {
            sb.append(".nq.gz");
        }
        return sb.toString();
    }

    private static void logInfo(List<InputFile> inputFiles) {
        StringBuilder sb = new StringBuilder();
        sb.append("Executing scripts on ").append(inputFiles.size()).append(" data set(s): ");
        for (Iterator<InputFile> it = inputFiles.iterator(); it.hasNext(); ) {
            if (inputFiles.size() > 1) sb.append("\n - ");
            sb.append(it.next().toString());
        }
        log.info(sb.toString());
    }
}

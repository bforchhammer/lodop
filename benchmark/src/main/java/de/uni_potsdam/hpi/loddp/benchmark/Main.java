package de.uni_potsdam.hpi.loddp.benchmark;

import de.uni_potsdam.hpi.loddp.benchmark.execution.InputFile;
import de.uni_potsdam.hpi.loddp.benchmark.execution.ScriptRunner;
import de.uni_potsdam.hpi.loddp.benchmark.execution.ScriptRunnerBuilder;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ReportGenerator;
import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.HadoopLocation;
import de.uni_potsdam.hpi.loddp.common.PigContextUtil;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScriptFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Main class.
 */
public class Main {

    protected static final String LOG_FILENAME_APACHE = "apache.log";
    protected static final String LOG_FILENAME_BENCHMARK = "benchmark.log";
    protected static final String LOG_FILENAME_REPORTING = "report.log";
    protected static final String JOB_GRAPH_DIRECTORY = "plans";
    protected static final String HDFS_WORKING_DIRECTORY = "";
    protected static final String HDFS_DATA_DIRECTORY = "data";
    protected static Log log;

    private static void initLogging(String jobName) {
        // Setup directory for log files (this will only work if executed before any logger is initialised.
        StringBuilder logDirectory = new StringBuilder("logs/");
        logDirectory.append(new DateTime().toString("YYYY-MM-dd-HH-mm-ss"));
        if (jobName != null && !jobName.isEmpty()) {
            logDirectory.append('-').append(jobName);
        }
        new File(logDirectory.toString()).mkdirs();
        System.setProperty("log.directory", logDirectory.toString());
        System.out.println("Logging to = " + System.getProperty("log.directory"));

        // Configure file names for log files.
        System.setProperty("log.filename.apache", LOG_FILENAME_APACHE);
        System.setProperty("log.filename.benchmark", LOG_FILENAME_BENCHMARK);
        System.setProperty("log.filename.reporting", LOG_FILENAME_REPORTING);

        // Initialize Logger.
        log = LogFactory.getLog(Main.class);
    }

    public static String getJobGraphDirectory() {
        return System.getProperty("log.directory") + '/' + JOB_GRAPH_DIRECTORY + '/';
    }

    private static Options getCliOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message.");
        options.addOption(OptionBuilder
            .withLongOpt("job-name")
            .withDescription("Name of the benchmark job. This is used for the name of the logging directory.")
            .hasArg()
            .withArgName("experiment-77")
            .create('n'));
        options.addOption(OptionBuilder
            .withLongOpt("scripts")
            .withDescription("Space-separated list of pig script file names to execute. Asterisk (*) can be used as a" +
                " wildcard.")
            .hasArgs()
            .withArgName("number_of_instances")
            .create('s'));
        options.addOption(OptionBuilder
            .withLongOpt("scripts-directory")
            .withDescription("Directory to load scripts from, relative to resource-path.")
            .hasArg()
            .withArgName("pig-queries")
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("inverse")
            .withDescription("Use this option to turn --scripts into a blacklist, i.e., " +
                "execute all scripts except the ones specified with the --scripts argument.")
            .hasArg(false)
            .create('i'));
        options.addOption(OptionBuilder
            .withLongOpt("cluster")
            .withDescription("Execute scripts on the HPI cluster (tenemhead2).")
            .hasArg(false)
            .create('c'));
        options.addOption(OptionBuilder
            .withLongOpt("datasets")
            .withDescription("Space-separated list of file names for datasets to be loaded. Datasets need to " +
                "manually be placed on HDFS. Relative file names are loaded from " +
                (HDFS_WORKING_DIRECTORY.isEmpty() ? "~/" : HDFS_WORKING_DIRECTORY) + HDFS_DATA_DIRECTORY +
                "/[dataset]. The file extension '.nq.gz' can be omitted.")
            .hasArgs().withArgName("dbpedia-1M")
            .create('d'));
        options.addOption(OptionBuilder
            .withLongOpt("explain")
            .withDescription("Dumps the logical, physical and map-reduce operator plans as graphs for each script.")
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
        options.addOption(OptionBuilder
            .withLongOpt("optimize-all")
            .withDescription("Apply all custom optimization rules.")
            .hasArg(false)
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("optimize-identicals")
            .withDescription("Apply 'MergeIdentical' optimization rule.")
            .hasArg(false)
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("optimize-filters")
            .withDescription("Apply 'CombineFilters' optimization rule.")
            .hasArg(false)
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("optimize-aggregations")
            .withDescription("Apply 'CombineForeach' optimization rule.")
            .hasArg(false)
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("output-directory")
            .withDescription("Output directory on HDFS to store results in.")
            .hasArg().withArgName("test-1")
            .create('o'));
        options.addOption(OptionBuilder
            .withLongOpt("no-output-override")
            .withDescription("By default, any existing output is removed before execution. Specify this option to " +
                "display an error message instead and skip the respective script.")
            .hasArg(false)
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("disable-pig-mqo")
            .withDescription("Disable Apache Pig's Multi-Query Optimization features. Can help with avoiding memory " +
                "errors for large sets of scripts.")
            .hasArg(false)
            .create());
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
        CommandLine cmd;
        String cmdLineSyntax = "./gradlew :benchmark:run -PappArgs=\"[args]\"";
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            new HelpFormatter().printHelp(cmdLineSyntax, options);
            return;
        }

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp(cmdLineSyntax, options);
            return;
        }

        // Setup logging.
        initLogging(cmd.getOptionValue("job-name"));

        ScriptRunnerBuilder builder = new ScriptRunnerBuilder();

        if (cmd.hasOption("disable-pig-mqo")) {
            PigContextUtil.disablePigMQO();
        }

        // Determine hadoop location, by default use localhost.
        if (cmd.hasOption("cluster")) {
            builder.setLocation(HadoopLocation.HPI_CLUSTER);
        }

        // Determine whether to repeat execution.
        if (cmd.hasOption("repeat")) {
            builder.setRepeat(Integer.parseInt(cmd.getOptionValue("repeat")));
        }

        // Determine whether to merge plans together.
        if (cmd.hasOption("merge")) {
            builder.setMerged(true);
        }

        // Determine whether merged plans should be optimized via custom rules (combine identical operators,
        // foreach operators, filters, ...)
        if (cmd.hasOption("optimize-all")) {
            builder.setOptimizeMerged(true);
            builder.setOptimizerCombineFilters(true);
            builder.setOptimizerCombineForeachs(true);
            //builder.setOptimizerIgnoreProjections(true);
        }
        if (cmd.hasOption("optimize-identicals")) {
            builder.setOptimizeMerged(true);
        }
        if (cmd.hasOption("optimize-filters")) {
            builder.setOptimizerCombineFilters(true);
        }
        if (cmd.hasOption("optimize-aggregations")) {
            builder.setOptimizerCombineForeachs(true);
        }

        // Determine output directory.
        if (cmd.hasOption("output-directory")) {
            builder.setHdfsOutputDirectory(normalizePath(cmd.getOptionValue("output-directory")));
        }

        // Determine whether we want to prevent deletion of previous outputs.
        if (cmd.hasOption("no-output-override")) {
            builder.setReplaceExistingResults(false);
        }

        // Determine whether to dump plans as graphs.
        if (cmd.hasOption("explain")) {
            builder.setExplainPlans(true);
            builder.setExplainOutputDirectory(getJobGraphDirectory());
        }

        // Determine scripts directory.
        if (cmd.hasOption("scripts-directory")) {
            // @todo convert PigScriptFactory into a "Loader" class following the Builder pattern.
            PigScriptFactory.setPigScriptsDirectory(cmd.getOptionValue("scripts-directory"));
        }

        // By default execute all scripts.
        List<PigScript> scripts;
        if (cmd.hasOption("scripts")) {
            boolean inverse = cmd.hasOption("inverse");
            scripts = PigScriptFactory.findPigScripts(cmd.getOptionValues("scripts"), inverse);
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
            inputFiles.add(new InputFile(normalizeDatasetFilename("dbpedia-1M")));
        }
        logInfo(inputFiles);

        // Build ScriptRunner and execute all the things.
        ScriptRunner runner = builder.build();
        List<ScriptStats> statistics = runner.execute(scripts, inputFiles);

        // Crunch some numbers based on collected statistics.
        ReportGenerator rg = new ReportGenerator(statistics);
        rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();
    }

    private static String normalizePath(String path) {
        // prepend "working directory" value unless we have an absolute path.
        if (!HDFS_WORKING_DIRECTORY.isEmpty() && FilenameUtils.getPrefixLength(path) <= 0) {
            StringBuilder sb = new StringBuilder(HDFS_WORKING_DIRECTORY);
            if (!HDFS_WORKING_DIRECTORY.endsWith("/")) sb.append("/");
            sb.append(path);
            return sb.toString();
        }
        return path;
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
        return normalizePath(sb.toString());
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

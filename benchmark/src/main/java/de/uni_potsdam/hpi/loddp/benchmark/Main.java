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

    private static Set<String> getBlackList(String[] scriptNames) {
        Set<String> blacklist = getBlackList(SCRIPT_LIST.NONE);
        for (String s : scriptNames)
            blacklist.remove(s);
        return blacklist;
    }

    private static Set<String> getBlackList(String scriptName) {
        Set<String> blacklist = getBlackList(SCRIPT_LIST.NONE);
        blacklist.remove(scriptName);
        return blacklist;
    }

    private static Set<String> getBlackList(SCRIPT_LIST type) {
        Set<String> blacklist = new HashSet<String>();
        switch (type) {
            case NO_COOC:
                blacklist.add("incoming_property_cooc");
                blacklist.add("property_cooc_by_entities");
                blacklist.add("property_cooc_by_urls");
                break;
            case ONLY_COOC:
                blacklist.add("classes_by_entity");
                blacklist.add("classes_by_url");
                blacklist.add("classes_by_tld");
                blacklist.add("classes_by_pld");
                //blacklist.add("incoming_property_cooc");
                blacklist.add("number_of_triples");
                blacklist.add("number_of_instances");
                //blacklist.add("property_cooc_by_entities");
                //blacklist.add("property_cooc_by_urls");
                blacklist.add("properties_by_entity");
                blacklist.add("properties_by_pld");
                blacklist.add("properties_by_statement");
                blacklist.add("properties_by_tld");
                blacklist.add("properties_by_url");
                blacklist.add("vocabularies_by_entity");
                blacklist.add("vocabularies_by_pld");
                blacklist.add("vocabularies_by_tld");
                blacklist.add("vocabularies_by_url");
                break;
            case NONE:
                blacklist.add("classes_by_entity");
                blacklist.add("classes_by_url");
                blacklist.add("classes_by_tld");
                blacklist.add("classes_by_pld");
                blacklist.add("incoming_property_cooc");
                blacklist.add("number_of_triples");
                blacklist.add("number_of_instances");
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
                blacklist.add("vocabularies_by_url");
                break;
        }
        return blacklist;
    }

    /**
     * Looks for scripts, and runs complete benchmark.
     *
     * @param args
     */
    public static void main(String[] args) {
        scripts = PigScriptHelper.findPigScripts();
        ScriptRunner runner = new ScriptRunner(ScriptRunner.HADOOP_LOCATION.LOCALHOST);

        run_smallTest(runner);
    }

    private static void run_smallTest(ScriptRunner runner) {
        Set<String> blacklist = getBlackList(new String[] {"number_of_instances", "classes_by_entity", "classes_by_url"});
        InputFileSet freebase = new InputFileSet("BTC2012/Freebase", "data/freebase-", 10, 10000000);
        runSequential(runner, scripts, freebase.getBySize(10000), blacklist);
        ReportGenerator rg = new ReportGenerator(statisticsCollection);
        rg.scalabilityReport();
        rg.scriptComparison();
        rg.featureRuntimeAnalysis();
    }

    private static void run_scalabilityDBPedia(ScriptRunner runner) {
        Set<String> blacklist = getBlackList(SCRIPT_LIST.NO_COOC);

        InputFileSet dbpedia = new InputFileSet("BTC2012/DBPedia", "data/dbpedia-", 1000000, 100000000);
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
     * Benchmark the given set of pig scripts, for multiple different input files.
     *
     * @param runner    A script runner.
     * @param scripts   A set of pig scripts.
     * @param inputs    The set of quad input files.
     * @param blacklist A list of script names to skip and not execute.
     */
    public static void runSequential(ScriptRunner runner, Set<PigScript> scripts, InputFileSet inputs, Set<String> blacklist) {
        for (InputFile file : inputs.getAll()) {
            runSequential(runner, scripts, file, blacklist);
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

    private static enum SCRIPT_LIST {
        ALL, NONE, ONLY_COOC, NO_COOC
    }
}

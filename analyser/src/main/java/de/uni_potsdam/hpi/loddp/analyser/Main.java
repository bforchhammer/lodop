package de.uni_potsdam.hpi.loddp.analyser;

import de.uni_potsdam.hpi.loddp.analyser.matching.LogicalPlanMatcher;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScript;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScriptFactory;
import de.uni_potsdam.hpi.loddp.common.OperatorCounter;
import de.uni_potsdam.hpi.loddp.common.PigContextUtil;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompilerException;
import de.uni_potsdam.hpi.loddp.common.printing.GraphvizDumper;
import de.uni_potsdam.hpi.loddp.common.printing.LOFilterPrinter;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScriptFactory;
import de.uni_potsdam.hpi.loddp.optimization.PlanOptimizerBuilder;
import de.uni_potsdam.hpi.loddp.optimization.merging.LogicalPlanMerger;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class Main {
    private static final Log log = LogFactory.getLog(Main.class);

    private static Options getCliOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print this help message.");
        options.addOption(OptionBuilder
            .withLongOpt("scripts")
            .withDescription("Space-separated list of pig script names to analyse. Asterisk (*) can be used as a wildcard.")
            .hasArgs()
            .withArgName("classes_*")
            .create('s'));
        options.addOption(OptionBuilder
            .withLongOpt("scripts-directory")
            .withDescription("Directory to load scripts from, relative to resource-path.")
            .hasArg()
            .withArgName("pig-queries")
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("inverse")
            .withDescription("Use --scripts as a blacklist, i.e., execute all scripts except the ones specified with --scripts.")
            .hasArg(false)
            .create('i'));
        options.addOption(OptionBuilder
            .withLongOpt("graphs")
            .withDescription("Output PNG graphs of plans for all analysed scripts.")
            .hasArg(false)
            .create('g'));
        options.addOption(OptionBuilder
            .withLongOpt("merge")
            .withDescription("Merge scripts and analyse the merged plan.")
            .hasArg(false)
            .create('m'));
        options.addOption(OptionBuilder
            .withLongOpt("analyse-preprocessing")
            .withDescription("Analyse common pre-processing.")
            .hasArg(false)
            .create('p'));
        options.addOption(OptionBuilder
            .withLongOpt("filter-dumper")
            .withDescription("Use a special graph output printer that dumps the expressions of filter operations.")
            .hasArg(false)
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("optimize-all")
            .withDescription("Apply all custom optimization rules.")
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
            .withLongOpt("count-operators")
            .withDescription("Count used operators in plans.")
            .hasArg(false)
            .create());
        return options;
    }

    protected static void countOperators(List<AnalysedScript> scripts) {
        OperatorCounter counter = new OperatorCounter("logical unoptimized");
        OperatorCounter optimizedCounter = new OperatorCounter("logical optimized");
        OperatorCounter physicalCounter = new OperatorCounter("physical");
        OperatorCounter mrCounter = new OperatorCounter("map-reduce");

        for (AnalysedScript script : scripts) {
            counter.count(script.getUnoptimizedLogicalPlan());
            optimizedCounter.count(script.getLogicalPlan());
            physicalCounter.count(script.getPhysicalPlan());
            mrCounter.count(script.getMapReducePlan());
        }
        counter.dump();
        optimizedCounter.dump();
        physicalCounter.dump();
        mrCounter.dump();
    }

    protected static void countOperators(String name, LogicalPlan plan) {
        OperatorCounter counter = new OperatorCounter(name);
        counter.count(plan);
        counter.dump();
    }

    protected static void countOperators(String name, MROperPlan plan) {
        OperatorCounter counter = new OperatorCounter(name);
        counter.count(plan);
        counter.dump();
    }

    public static void main(String[] args) throws IOException {
        Options options = getCliOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine cmd;
        String cmdLineSyntax = "./gradlew :analyser:run -PappArgs=\"[args]\"";
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

        boolean dumpPlansAsGraphs = false;
        if (cmd.hasOption("graphs")) {
            dumpPlansAsGraphs = true;
        }

        // Determine scripts directory.
        if (cmd.hasOption("scripts-directory")) {
            // @todo convert PigScriptFactory into a "Loader" class following the Builder pattern.
            PigScriptFactory.setPigScriptsDirectory(cmd.getOptionValue("scripts-directory"));
        }

        PigContext pigContext = PigContextUtil.getContext();
        pigContext.inExplain = true; // Used to skip some validation rules, e.g. checking of input/output files.
        pigContext.connect();

        // By default execute all scripts.
        List<PigScript> scripts;
        if (cmd.hasOption("scripts")) {
            boolean inverse = cmd.hasOption("inverse");
            scripts = PigScriptFactory.findPigScripts(cmd.getOptionValues("scripts"), inverse);
        } else {
            scripts = PigScriptFactory.findPigScripts();
        }

        boolean findCommonPreprocessing = cmd.hasOption("analyse-preprocessing");
        boolean mergePlans = cmd.hasOption("merge");

        // Analyse scripts.
        List<AnalysedScript> analysedScripts = AnalysedScriptFactory.analyse(scripts, dumpPlansAsGraphs, pigContext);

        if (cmd.hasOption("count-operators")) {
            countOperators(analysedScripts);
        }

        // Compare logical plans and try to find common pre-processing steps.
        if (findCommonPreprocessing && analysedScripts.size() > 1) {
            LogicalPlanMatcher.findCommonPreprocessing(analysedScripts, false);
        }

        // Merge plans into one monster plan.
        LogicalPlan mergedPlan;
        if (mergePlans && analysedScripts.size() > 0) {
            LogicalPlanMerger merger = new LogicalPlanMerger();
            for (AnalysedScript script : analysedScripts) {
                LogicalPlan plan = script.getUnoptimizedLogicalPlan();
                if (plan != null) merger.merge(plan);
            }
            mergedPlan = merger.getMergedPlan();

            GraphvizDumper dumper = new GraphvizDumper("dot/");
            if (cmd.hasOption("filter-dumper")) {
                dumper.setPlanDumper(LogicalPlan.class, LOFilterPrinter.class);
            }

            dumper.setFilenamePrefix("all-merged-");
            dumper.print(mergedPlan);
            countOperators("merged", mergedPlan);

            //countMROperators(pigContext, mergedPlan);

            // Apply our optimization rules to merged plan.
            PlanOptimizerBuilder optimizer = new PlanOptimizerBuilder(false);
            if (cmd.hasOption("optimize-all") || cmd.hasOption("optimize-filters")) {
                optimizer.setCombineFilters(true);
            }
            if (cmd.hasOption("optimize-all") || cmd.hasOption("optimize-aggregations")) {
                optimizer.setCombineForeachs(true);
            }

            optimizer.getInstance(mergedPlan).optimize();
            dumper.print(mergedPlan, "-optimized");
            countOperators("merged-optimized", mergedPlan);

            //countMROperators(pigContext, mergedPlan);

            // Apply Pig's default optimization rules to merged plan.
            new org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer(mergedPlan, 100,
                new HashSet<String>()).optimize();
            dumper.print(mergedPlan, "-optimized-fully");
            countOperators("merged-optimized-fully", mergedPlan);
        }
    }

    private static void countMROperators(PigContext pigContext, LogicalPlan mergedPlan) {
        // count MR jobs
        ScriptCompiler mergedCompiler = new ScriptCompiler(pigContext, mergedPlan, false);
        try {
            countOperators("Merged MR jobs", mergedCompiler.getMapReducePlan());
        } catch (ScriptCompilerException e) {
            log.error("Meh", e);
        }

    }
}

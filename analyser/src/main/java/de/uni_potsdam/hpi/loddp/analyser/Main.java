package de.uni_potsdam.hpi.loddp.analyser;

import de.uni_potsdam.hpi.loddp.analyser.matching.LogicalPlanMatcher;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScript;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScriptFactory;
import de.uni_potsdam.hpi.loddp.common.PigContextUtil;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScriptFactory;
import de.uni_potsdam.hpi.loddp.optimization.merging.LogicalPlanMerger;
import de.uni_potsdam.hpi.loddp.optimization.merging.MergedLogicalPlan;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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
        return options;
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
        Set<PigScript> scripts = null;
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

        // Compare logical plans and try to find common pre-processing steps.
        if (findCommonPreprocessing && analysedScripts.size() > 1) {
            LogicalPlanMatcher.findCommonPreprocessing(analysedScripts, false);
        }

        // Merge plans into one monster plan.
        MergedLogicalPlan mergedPlan = null;
        if (mergePlans && analysedScripts.size() > 0) {
            LogicalPlanMerger merger = new LogicalPlanMerger();
            for (AnalysedScript script : analysedScripts) {
                LogicalPlan plan = script.getUnoptimizedLogicalPlan();
                if (plan != null) merger.merge(plan);
            }
            mergedPlan = merger.getMergedPlan();
            mergedPlan.dumpAsGraph("dot/all-merged-logical.dot");
        }
    }
}

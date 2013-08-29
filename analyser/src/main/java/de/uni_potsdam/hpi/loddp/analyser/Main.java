package de.uni_potsdam.hpi.loddp.analyser;

import de.uni_potsdam.hpi.loddp.analyser.matching.LogicalPlanMatcher;
import de.uni_potsdam.hpi.loddp.analyser.merging.LogicalPlanMerger;
import de.uni_potsdam.hpi.loddp.analyser.merging.MergedLogicalPlan;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScript;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScriptFactory;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScriptFactory;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
            .withLongOpt("graphs")
            .withDescription("Output PNG graphs of plans for all analysed scripts.")
            .hasArg(false)
            .create('g')
        );
        return options;
    }

    public static void main(String[] args) throws IOException {
        Options options = getCliOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
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

        // By default execute all scripts.
        Set<PigScript> scripts = null;
        if (cmd.hasOption("scripts")) {
            scripts = PigScriptFactory.findPigScripts(cmd.getOptionValues("scripts"));
        } else {
            scripts = PigScriptFactory.findPigScripts();
        }

        // Analyse scripts.
        List<AnalysedScript> analysedScripts = AnalysedScriptFactory.analyse(scripts, dumpPlansAsGraphs);

        boolean analysePreprocessing = false;
        boolean mergeScripts = true;

        // Compare logical plans and try to find common pre-processing steps.
        if (analysePreprocessing && analysedScripts.size() > 2) {
            LogicalPlanMatcher.findCommonPreprocessing(analysedScripts, false);
        }

        if (mergeScripts && analysedScripts.size() > 1) {
            MergedLogicalPlan plan = LogicalPlanMerger.merge(analysedScripts, false);
            plan.dumpAsGraph();
        }
    }
}

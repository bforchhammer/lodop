package de.uni_potsdam.hpi.loddp.analyser;

import de.uni_potsdam.hpi.loddp.analyser.matching.LogicalPlanMatcher;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScript;
import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScriptFactory;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScriptFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class Main {
    protected static final Log log;

    static {
        System.out.println();
        log = LogFactory.getLog(Main.class);
    }

    public static void main(String[] args) throws IOException {
        boolean dumpPlansAsGraphs = false;

        // Load a set of scripts.
        Set<PigScript> scripts = PigScriptFactory.findPigScripts();

        // Analyse scripts.
        List<AnalysedScript> analysedScripts = AnalysedScriptFactory.analyse(scripts, dumpPlansAsGraphs);

        // Compare logical plans and try to find common pre-processing steps.
        if (analysedScripts.size() > 2) {
            LogicalPlanMatcher.findCommonPreprocessing(analysedScripts);
        }
    }
}

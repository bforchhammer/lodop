package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.PigContextUtil;
import de.uni_potsdam.hpi.loddp.common.ScriptCompilationException;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AnalysedScriptFactory {
    private static final Log log = LogFactory.getLog(AnalysedScriptFactory.class);
    private static ScriptAnalyser scriptAnalyser = null;

    protected static ScriptAnalyser getScriptAnalyser(PigContext pigContext) throws IOException {
        if (scriptAnalyser == null) {
            scriptAnalyser = new ScriptAnalyser(pigContext);
        }
        return scriptAnalyser;
    }

    protected static PigContext createPigContext() throws IOException {
        PigContext pigContext = PigContextUtil.getContext();

        // Used to skip some validation rules, e.g. checking of input/output files.
        pigContext.inExplain = true;

        // Initialise connection to local Hadoop instance.
        pigContext.connect();

        return pigContext;
    }

    public static List<AnalysedScript> analyse(Set<PigScript> scripts) throws IOException {
        return analyse(scripts, false, createPigContext());
    }

    public static List<AnalysedScript> analyse(Set<PigScript> scripts, PigContext pigContext) throws IOException {
        return analyse(scripts, false, pigContext);
    }

    public static List<AnalysedScript> analyse(Set<PigScript> scripts, boolean dumpGraphs) throws IOException {
        return analyse(scripts, dumpGraphs, createPigContext());
    }

    public static List<AnalysedScript> analyse(Set<PigScript> scripts, boolean dumpGraphs, PigContext pigContext) throws IOException {
        log.info("Analysing " + scripts.size() + " scripts.");
        List<AnalysedScript> analysed = new ArrayList<AnalysedScript>();
        Iterator<PigScript> iterator = scripts.iterator();
        while (iterator.hasNext()) {
            PigScript script = iterator.next();
            log.debug("Analysing script: " + script.getScriptName());
            try {
                AnalysedScript a = getScriptAnalyser(pigContext).analyse(script);
                analysed.add(a);
                if (dumpGraphs) {
                    a.dumpPlansAsGraphs();
                }
            } catch (ScriptCompilationException e) {
                log.error("Could not analyse script: " + script.getScriptName(), e);
            }
        }
        log.info("Script analysis complete.");
        return analysed;
    }
}

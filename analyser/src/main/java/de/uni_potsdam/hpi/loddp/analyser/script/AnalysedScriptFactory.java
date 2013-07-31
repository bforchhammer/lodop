package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AnalysedScriptFactory {
    private static final Log log = LogFactory.getLog(AnalysedScriptFactory.class);
    private static ScriptAnalyser scriptAnalyser = null;

    protected static ScriptAnalyser getScriptAnalyser() throws IOException {
        if (scriptAnalyser == null) {
            scriptAnalyser = new ScriptAnalyser();
        }
        return scriptAnalyser;
    }

    public static List<AnalysedScript> analyse(Set<PigScript> scripts) throws IOException {
        return analyse(scripts, false);
    }

    public static List<AnalysedScript> analyse(Set<PigScript> scripts, boolean dumpGraphs) throws IOException {
        List<AnalysedScript> analysed = new ArrayList<AnalysedScript>();
        Iterator<PigScript> iterator = scripts.iterator();
        while (iterator.hasNext()) {
            PigScript script = iterator.next();
            try {
                AnalysedScript a = getScriptAnalyser().analyse(script);
                analysed.add(a);
                if (dumpGraphs) {
                    a.dumpPlansAsGraphs();
                }
            } catch (IOException e) {
                log.error("Could not analyse script: " + script.getScriptName(), e);
            }
        }
        return analysed;
    }
}

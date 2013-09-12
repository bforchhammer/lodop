package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
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

    public static List<AnalysedScript> analyse(Set<PigScript> scripts, PigContext pigContext) throws IOException {
        return analyse(scripts, false, pigContext);
    }

    public static List<AnalysedScript> analyse(Set<PigScript> scripts, boolean dumpGraphs, PigContext pigContext) throws IOException {
        log.info("Analysing " + scripts.size() + " scripts.");
        List<AnalysedScript> analysed = new ArrayList<AnalysedScript>();
        Iterator<PigScript> iterator = scripts.iterator();
        while (iterator.hasNext()) {
            PigScript script = iterator.next();
            log.debug("Analysing script: " + script.getScriptName());
            ScriptCompiler scriptCompiler = new ScriptCompiler(pigContext, script, "fake-input.nq.gz", "fake-output");
            AnalysedScript a = new AnalysedScript(script, scriptCompiler);
            analysed.add(a);
            if (dumpGraphs) {
                a.dumpPlansAsGraphs();
            }
        }
        log.info("Script analysis complete.");
        return analysed;
    }
}

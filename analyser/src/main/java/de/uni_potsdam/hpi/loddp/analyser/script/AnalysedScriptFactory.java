package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompilerException;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class AnalysedScriptFactory {
    private static final Log log = LogFactory.getLog(AnalysedScriptFactory.class);

    public static List<AnalysedScript> analyse(List<PigScript> scripts, PigContext pigContext) throws IOException {
        return analyse(scripts, false, pigContext);
    }

    public static List<AnalysedScript> analyse(Collection<PigScript> scripts, boolean dumpGraphs,
                                               PigContext pigContext) throws IOException {
        log.info("Analysing " + scripts.size() + " scripts.");
        List<AnalysedScript> analysed = new ArrayList<AnalysedScript>();
        Iterator<PigScript> iterator = scripts.iterator();
        while (iterator.hasNext()) {
            PigScript script = iterator.next();
            log.debug("Analysing script: " + script.getScriptName());
            try {
                ScriptCompiler scriptCompiler = new ScriptCompiler(pigContext, script, "fake-input.nq.gz", script.getScriptName());
                scriptCompiler.getLogicalPlan(); // Trigger compile errors.
                AnalysedScript a = new AnalysedScript(script, scriptCompiler);
                analysed.add(a);
                if (dumpGraphs) {
                    a.dumpPlansAsGraphs();
                }
            } catch (ScriptCompilerException e) {
                log.error("Failed to compile script " + script.getScriptName(), e);
            }
        }
        log.info("Script analysis complete.");
        return analysed;
    }
}

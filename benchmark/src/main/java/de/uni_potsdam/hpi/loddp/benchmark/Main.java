package de.uni_potsdam.hpi.loddp.benchmark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Main class.
 */
public class Main {

    protected static final Log log = LogFactory.getLog(Main.class);

    /**
     * Looks for scripts, and runs complete benchmark.
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println();

        ScriptRunner runner = null;
        try {
            runner = new ScriptRunner();
        } catch (IOException e) {
            log.error("Failed to create ScriptRunner", e);
            return;
        }

        Set<PigScript> scripts = PigScriptHelper.findPigScripts();
        for (PigScript script : scripts) {
            System.out.print(script);
            if (script.getScriptName().equals("number_of_triples")) {
                System.out.println(" - RUNNING");
                runner.runScript(script);
            } else {
                System.out.println(" - SKIPPED");
            }
        }

        // Shutdown pig server.
        runner.shutdown();
    }

}

package de.uni_potsdam.hpi.loddp.benchmark;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.IOException;
import java.util.HashSet;
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
        Set<PigScript> scripts = PigScriptHelper.findPigScripts();

        Set<String> blacklist = new HashSet<String>();
        blacklist.add("classes_by_entity");
        blacklist.add("classes_by_url");
        blacklist.add("classes_by_tld");
        blacklist.add("classes_by_pld");
        blacklist.add("incoming_property_cooc");
        blacklist.add("number_of_triples");
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

        runSequential(scripts, blacklist);
    }

    public static void runSequential(Set<PigScript> scripts) {
        runSequential(scripts, new HashSet<String>());
    }

    public static void runSequential(Set<PigScript> scripts, Set<String> blacklist) {
        for (PigScript script : scripts) {
            System.out.print(script);
            if (blacklist.contains(script.getScriptName())) {
                System.out.println(" - SKIPPED");
            } else {
                System.out.println(" - RUNNING");
                runScript(script);
            }
        }
    }

    public static void runScript(PigScript script) {
        ScriptRunner runner = null;

        // Create pig server.
        try {
            runner = new ScriptRunner();
        } catch (IOException e) {
            log.error("Failed to create ScriptRunner.", e);
            return;
        }

        // Execute script.
        PigStats stats = runner.runScript(script);
        System.out.println(String.format("Pig job took %s.", DurationFormatUtils.formatDurationHMS(stats.getDuration())));

        // Shutdown pig server.
        runner.shutdown();
    }

}


package de.uni_potsdam.hpi.loddp.benchmark;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.IOException;
import java.util.Properties;

/**
 * Main class.
 */
public class Main {

    private PigServer pig;

    protected Main() throws IOException {
        this.pig = new PigServer(ExecType.MAPREDUCE);

        // Register UDF + required libraries.
        this.pig.registerJar("ldif-single-0.5.1-jar-with-dependencies.jar");
        this.pig.registerJar("loddesc-core-0.1.jar");
    }

    protected void runScript() throws IOException {
        if (this.pig.existsFile("results-topClassesByEntities")) {
            this.pig.deleteFile("results-topClassesByEntities");
            System.out.println("Previous output files deleted");
        }
        this.pig.registerScript(ClassLoader.getSystemClassLoader().getResourceAsStream("classes_by_entity.pig"));
        ExecJob job = this.pig.store("topClassesByEntities", "results-topClassesByEntities");
        PigStats stats = job.getStatistics();
        System.out.println(String.format("Pig job took %dms.", stats.getDuration()));
    }

    protected void shutdown() {
        if (this.pig != null) {
            this.pig.shutdown();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println();
        Main main = null;
        try {
            main = new Main();
            main.runScript();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (main != null) main.shutdown();
        }
    }

}

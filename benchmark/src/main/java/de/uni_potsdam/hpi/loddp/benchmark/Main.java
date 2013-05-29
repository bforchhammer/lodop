package de.uni_potsdam.hpi.loddp.benchmark;


import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * Main class containing main().
 */
public class Main {

    public static void main(String[] args) {
        String[] pig_args = new String[] {
                "classes_by_entity.pig"
        };
        PigStats stats = PigRunner.run(pig_args, new PigProgressListener());
    }

}

package de.uni_potsdam.hpi.loddp.benchmark.execution;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InputFileHelper {

    protected static final Log log = LogFactory.getLog(InputFileHelper.class);

    /**
     * Tries to determine the tuple count from the given filename.
     *
     * Examples: <ul> <li>./data/dbpedia-10000.nq.gz => 10,000</li> <li>./data/dbpedia-100K.nq.gz => 100,000</li>
     * <li>./data/dbpedia-1M.nq.gz => 1,000,000</li> </ul>
     *
     * @param filename
     *
     * @return
     */
    public static long guessTupleCount(String filename) {
        // Remove extension twice to get rid of he full extension ".nq.gz".
        String name = FilenameUtils.removeExtension(FilenameUtils.removeExtension(FilenameUtils.getName(filename)));
        int lastDashIdx = name.lastIndexOf('-');
        if (lastDashIdx != -1) {
            String numberPart = name.substring(lastDashIdx + 1);
            String number = numberPart.replace("K", "000").replace("M", "000000");
            try {
                return Long.parseLong(number);
            } catch (NumberFormatException e) {
                log.warn(String.format("Could not determine tuplesize from filename %s.", filename), e);
            }
        }
        return 0L;
    }

    /**
     * Tries to guess the name of the dataset from the given filename.
     *
     * Examples:<ul> <li>./data/dbpedia-1M.nq.gz => dbpedia</li> <li>./data/freebase-1M.nq.gz => freebase</li> </ul>
     *
     * @param filename
     *
     * @return
     */
    public static String guessFilesetIdentifier(String filename) {
        String name = FilenameUtils.getBaseName(filename);
        int lastDashIdx = name.lastIndexOf('-');
        if (lastDashIdx != -1) {
            return name.substring(0, lastDashIdx);
        }
        return "N/A";
    }
}

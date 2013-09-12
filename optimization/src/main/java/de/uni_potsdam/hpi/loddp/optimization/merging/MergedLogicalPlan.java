package de.uni_potsdam.hpi.loddp.optimization.merging;

import de.uni_potsdam.hpi.loddp.common.GraphvizUtil;
import de.uni_potsdam.hpi.loddp.common.LogicalPlanPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.PlanDumper;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Constructor;

/**
 * A merged logical plan.
 *
 * @see LogicalPlanMerger
 */
public class MergedLogicalPlan extends LogicalPlan {
    protected static final Log log = LogFactory.getLog(MergedLogicalPlan.class);
    protected static boolean defaultVerbosePlanPrinting = false;

    /**
     * Dump the plan to `dot/all-merged-logical.png` (requires GraphViz to be installed).
     */
    public void dumpAsGraph() {
        dumpAsGraph("dot/all-merged-logical.dot");
    }

    /**
     * Dump the plan to the given relative filename.
     *
     * @param dotFilename Filename for the dot file, needs to end in ".dot".
     */
    public void dumpAsGraph(String dotFilename) {
        dumpAsGraph(dotFilename, defaultVerbosePlanPrinting);
    }

    public void dumpAsGraph(String dotFilename, boolean verbose) {
        dumpAsGraph(dotFilename, LogicalPlanPrinter.class, verbose);
    }

    public void dumpAsGraph(String dotFilename, Class<? extends PlanDumper> printerClass) {
        dumpAsGraph(dotFilename, printerClass, defaultVerbosePlanPrinting);
    }

    public void dumpAsGraph(String dotFilename, Class<? extends PlanDumper> printerClass, boolean verbose) {
        try {
            File dotFile = new File(dotFilename);
            dotFile.getParentFile().mkdirs();
            Constructor<? extends PlanDumper> ctr = printerClass.getConstructor(BaseOperatorPlan.class, PrintStream.class);
            PlanDumper dumper = ctr.newInstance(this, new PrintStream(dotFile));
            dumper.setVerbose(verbose);
            dumper.dump();
            GraphvizUtil.convertToImage("png", dotFile);
        } catch (Exception e) {
            log.warn("Could not dump merged plan as graph.", e);
        }
    }
}

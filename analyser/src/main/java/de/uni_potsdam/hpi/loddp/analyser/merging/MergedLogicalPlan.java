package de.uni_potsdam.hpi.loddp.analyser.merging;

import de.uni_potsdam.hpi.loddp.common.GraphvizUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.DotLOPrinter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

/**
 * A merged logical plan.
 *
 * Can only contain {@link MergedOperator merged operators}.
 *
 * @see LogicalPlanMerger
 */
public class MergedLogicalPlan extends LogicalPlan {
    protected static final Log log = LogFactory.getLog(MergedLogicalPlan.class);

    /**
     * Add a new operator, making sure that it's of type MergedOperator.
     *
     * @param op
     */
    @Override
    public void add(Operator op) {
        // Make sure, only instances of "MergedOperator" are added to this logical plan.
        if (op instanceof MergedOperator) {
            super.add(op);
        } else {
            throw new IllegalArgumentException("Cannot add operator. Expected class: " + MergedOperator.class
                .getCanonicalName() + ". Given class: " + op.getClass());
        }
    }

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
        try {
            File dotFile = new File(dotFilename);
            dotFile.getParentFile().mkdirs();
            new DotLOPrinter(this, new PrintStream(dotFile)).dump();
            GraphvizUtil.convertToImage("png", dotFile);
        } catch (IOException e) {
            log.warn("Could not dump merged plan as graph.", e);
        }
    }
}

package de.uni_potsdam.hpi.loddp.common.printing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanDumper;

import java.io.PrintStream;
import java.util.Set;

/**
 * Simple printer for logical plans. Only outputs operator names, Plan orientation is top-to-bottom.
 */
public class LOSimplePrinter extends LogicalPlanPrinter {
    protected static final Log log = LogFactory.getLog(LogicalPlanPrinter.class);

    public LOSimplePrinter(BaseOperatorPlan plan, PrintStream ps) {
        super(plan, ps);
    }

    public LOSimplePrinter(BaseOperatorPlan plan, PrintStream ps, boolean isSubGraph, Set<Operator> mSubgraphs, Set<Operator> mMultiOutputSubgraphs, Set<Operator> mMultiInputSubgraphs) {
        super(plan, ps, isSubGraph, mSubgraphs, mMultiOutputSubgraphs, mMultiInputSubgraphs);
    }

    @Override
    protected PlanDumper makeDumper(BaseOperatorPlan plan, PrintStream ps) {
        return new LOSimplePrinter(plan, ps, true, mSubgraphs, mMultiInputSubgraphs, mMultiOutputSubgraphs);
    }

    /**
     * Used to generate the label for an operator.
     *
     * @param op operator to dump
     */
    @Override
    protected String getName(Operator op) {
        return op.getName().substring(2).toUpperCase();
    }
}

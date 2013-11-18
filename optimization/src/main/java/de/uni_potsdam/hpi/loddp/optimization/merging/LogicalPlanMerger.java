package de.uni_potsdam.hpi.loddp.optimization.merging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.AllSameRalationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.*;

import java.util.List;

/**
 * Merges a set of logical plans into one large logical plan.
 */
public class LogicalPlanMerger {
    protected static final Log log = LogFactory.getLog(LogicalPlanMerger.class);
    private LogicalPlan mergedPlan;

    public LogicalPlanMerger() {
        mergedPlan = new LogicalPlan();
    }

    public static LogicalPlan merge(List<LogicalPlan> plans) {
        LogicalPlanMerger merger = new LogicalPlanMerger();
        log.info(String.format("Merging %d logical plans.", plans.size()));
        for (LogicalPlan plan : plans) {
            try {
                merger.merge(plan);
            } catch (FrontendException e) {
                log.error("Error while trying to merge plans.", e);
            }
        }
        return merger.getMergedPlan();
    }

    /**
     * Merge the given logical plan into this plan.
     *
     * @param plan
     */
    public void merge(LogicalPlan plan) throws FrontendException {
        new Merger(plan).visit();
    }

    /**
     * Retrieve the merged logical plan.
     *
     * @return The merged logical plan
     */
    public LogicalPlan getMergedPlan() {
        return mergedPlan;
    }

    protected class Merger extends AllSameRalationalNodesVisitor {
        public Merger(OperatorPlan plan) throws FrontendException {
            super(plan, new DependencyOrderWalker(plan));
        }

        /**
         * Merge the given operator into the merged plan.
         *
         * @param operator
         */
        @Override
        protected void execute(LogicalRelationalOperator operator) throws FrontendException {
            List<Operator> predecessors = operator.getPlan().getPredecessors(operator);
            List<Operator> softPredecessors = operator.getPlan().getSoftLinkPredecessors(operator);

            mergedPlan.add(operator);
            operator.setPlan(mergedPlan);
            if (predecessors != null) {
                for (Operator predecessor : predecessors) {
                    mergedPlan.connect(predecessor, operator);
                }
            }
            if (softPredecessors != null) {
                for (Operator predecessor : softPredecessors) {
                    mergedPlan.createSoftLink(predecessor, operator);
                }
            }
        }

        @Override
        public void visit(LOInnerLoad load) throws FrontendException {
            execute(load);
        }

        @Override
        public void visit(LOGenerate gen) throws FrontendException {
            execute(gen);
        }

        @Override
        public void visit(LOCube cube) throws FrontendException {
            execute(cube);
        }

        @Override
        public void visit(LOLimit loLimit) throws FrontendException {
            execute(loLimit);
        }

        @Override
        public void visit(LONative nativeMR) throws FrontendException {
            execute(nativeMR);
        }
    }
}

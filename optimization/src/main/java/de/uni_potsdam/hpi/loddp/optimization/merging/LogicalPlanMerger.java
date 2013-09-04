package de.uni_potsdam.hpi.loddp.optimization.merging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.*;

import java.util.*;

/**
 * Merges a set of logical plans into one large logical plan.
 */
public class LogicalPlanMerger {
    protected static final Log log = LogFactory.getLog(LogicalPlanMerger.class);
    private MergedLogicalPlan mergedPlan;
    private Map<Operator, Operator> mergedOperators;

    public LogicalPlanMerger() {
        mergedPlan = new MergedLogicalPlan();
        mergedOperators = new HashMap<Operator, Operator>();
    }

    /**
     * Merge (optimized) logical plans for all given logical plans into one logical plan.
     *
     * @param plans A set of logical plans.
     *
     * @return The merged logical plan.
     */
    public static MergedLogicalPlan merge(List<LogicalPlan> plans) {
        LogicalPlanMerger merger = new LogicalPlanMerger();
        int originalPlanSize = 0;
        log.info(String.format("Merging %d logical plans.", plans.size()));
        for (LogicalPlan plan : plans) {
            originalPlanSize += plan.size();
            try {
                merger.merge(plan);
            } catch (FrontendException e) {
                log.error("Error while trying to merge plans.", e);
            }
        }
        MergedLogicalPlan plan = merger.getMergedPlan();
        log.info(String.format("Merging complete. Merged plan contains %d of %d original operators.", plan.size(), originalPlanSize));
        return merger.getMergedPlan();
    }

    /**
     * Merge the given logical plan into this plan.
     *
     * @param plan
     */
    public void merge(LogicalPlan plan) throws FrontendException {
        new Visitor(plan).visit();
    }

    /**
     * Retrieve the merged logical plan.
     *
     * @return The merged logical plan
     */
    public MergedLogicalPlan getMergedPlan() {
        return mergedPlan;
    }

    /**
     * Visits all relational nodes in the given plan in dependency order and adds each visited node to the merged
     * logical plan.
     */
    private class Visitor extends LogicalRelationalNodesVisitor {
        private Visitor(OperatorPlan plan) throws FrontendException {
            super(plan, new DependencyOrderWalker(plan));
        }

        /**
         * Merge the given operator into the merged plan.
         *
         * @param operator
         */
        public void merge(Operator operator) {
            // Look up predecessors in merged plan and check whether they have any common successors in the merged
            // plan. If yes, one of them may be equivalent to the operator which we are currently trying to merge in.
            Collection<Operator> candidates = new ArrayList<Operator>(mergedOperators.values());
            List<Operator> predecessors = operator.getPlan().getPredecessors(operator);
            List<Operator> mergedPredecessors = new ArrayList<Operator>();
            if (predecessors != null) {
                for (Operator predecessor : predecessors) {
                    // Retrieve respective merged operator.
                    Operator mergedPredecessor = mergedOperators.get(predecessor);
                    if (mergedPredecessor == null) {
                        // This visitor walks the plan in dependency order so all predecessors should have already
                        // been merged in and this should therefore never happen.
                        throw new RuntimeException("Merged operator not found.");
                    }
                    mergedPredecessors.add(mergedPredecessor);
                    // Only candidates which are successors of ALL predecessors are valid.
                    List<Operator> successors = mergedPlan.getSuccessors(mergedPredecessor);
                    if (successors == null) candidates.clear();
                    else candidates.retainAll(successors);
                }
            }

            List<Operator> softPredecessors = operator.getPlan().getSoftLinkPredecessors(operator);
            List<Operator> mergedSoftPredecessors = new ArrayList<Operator>();
            if (softPredecessors != null) {
                for (Operator predecessor : softPredecessors) {
                    // Retrieve respective merged operator.
                    Operator mergedPredecessor = mergedOperators.get(predecessor);
                    if (mergedPredecessor == null) {
                        // This visitor walks the plan in dependency order so all predecessors should have already
                        // been merged in and this should therefore never happen.
                        throw new RuntimeException("Merged operator not found.");
                    }
                    mergedSoftPredecessors.add(mergedPredecessor);
                    // Only candidates which are successors of ALL predecessors are valid.
                    List<Operator> successors = mergedPlan.getSoftLinkSuccessors(mergedPredecessor);
                    if (successors == null) candidates.clear();
                    else candidates.retainAll(successors);
                }
            }

            // Compare each of the candidates with the given operator. If we can merge the given operator with one of
            // the candidates, then we are done.
            for (Operator candidate : candidates) {
                if (MergedOperatorFacade.decorate(candidate).merge(operator)) {
                    mergedOperators.put(operator, candidate);
                    return;
                }
            }

            // If we get to this point, then we haven't found a matching operator in the merged plan. We therefore
            // simply add the operator to the merged plan.
            mergedOperators.put(operator, operator);
            mergedPlan.add(operator);
            operator.setPlan(mergedPlan);
            for (Operator predecessor : mergedPredecessors) {
                mergedPlan.connect(predecessor, operator);
            }
            for (Operator predecessor : mergedSoftPredecessors) {
                mergedPlan.createSoftLink(predecessor, operator);
            }
        }

        @Override
        public void visit(LOLoad load) throws FrontendException {
            merge(load);
        }

        @Override
        public void visit(LOFilter filter) throws FrontendException {
            merge(filter);
        }

        @Override
        public void visit(LOStore store) throws FrontendException {
            merge(store);
        }

        @Override
        public void visit(LOJoin join) throws FrontendException {
            merge(join);
        }

        @Override
        public void visit(LOForEach foreach) throws FrontendException {
            merge(foreach);
        }

        @Override
        public void visit(LOGenerate gen) throws FrontendException {
            merge(gen);
        }

        @Override
        public void visit(LOInnerLoad load) throws FrontendException {
            merge(load);
        }

        @Override
        public void visit(LOCube cube) throws FrontendException {
            merge(cube);
        }

        @Override
        public void visit(LOCogroup loCogroup) throws FrontendException {
            merge(loCogroup);
        }

        @Override
        public void visit(LOSplit loSplit) throws FrontendException {
            merge(loSplit);
        }

        @Override
        public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
            merge(loSplitOutput);
        }

        @Override
        public void visit(LOUnion loUnion) throws FrontendException {
            merge(loUnion);
        }

        @Override
        public void visit(LOSort loSort) throws FrontendException {
            merge(loSort);
        }

        @Override
        public void visit(LORank loRank) throws FrontendException {
            merge(loRank);
        }

        @Override
        public void visit(LODistinct loDistinct) throws FrontendException {
            merge(loDistinct);
        }

        @Override
        public void visit(LOLimit loLimit) throws FrontendException {
            merge(loLimit);
        }

        @Override
        public void visit(LOCross loCross) throws FrontendException {
            merge(loCross);
        }

        @Override
        public void visit(LOStream loStream) throws FrontendException {
            merge(loStream);
        }

        @Override
        public void visit(LONative nativeMR) throws FrontendException {
            merge(nativeMR);
        }
    }

}

package de.uni_potsdam.hpi.loddp.optimization.merging.rules;

import de.uni_potsdam.hpi.loddp.common.OperatorPlanUtil;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.optimizer.AllSameRalationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Base class for merging rules.
 *
 * Merging rules always act on siblings of the same operator type.
 */
public abstract class MergingRule extends Rule {
    private Class<? extends LogicalRelationalOperator> typeRestriction;

    protected MergingRule(String n) {
        this(n, null);
    }

    protected MergingRule(String n, Class<? extends LogicalRelationalOperator> typeRestriction) {
        this(n, typeRestriction, false);
    }

    protected MergingRule(String n, Class<? extends LogicalRelationalOperator> typeRestriction, boolean mandatory) {
        super(n, mandatory);
        this.typeRestriction = typeRestriction;
    }

    public List<OperatorPlan> match(OperatorPlan plan) {
        currentPlan = plan;
        try {
            SiblingListCreator siblingListCreator = new SiblingListCreator(plan);
            siblingListCreator.visit();
            return siblingListCreator.getSiblingsLists();
        } catch (FrontendException e) {
            log.error("Failed to generate list of siblings.", e);
            return null;
        }
    }

    @Override
    protected OperatorPlan buildPattern() {
        return null;
    }

    protected class SiblingListCreator extends AllSameRalationalNodesVisitor {
        private List<Operator> seenOperators = new ArrayList<Operator>();
        private List<OperatorPlan> siblingsLists = new ArrayList<OperatorPlan>();
        private Class<? extends LogicalRelationalOperator> typeRestriction;

        public SiblingListCreator(OperatorPlan plan) throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }

        private boolean isValidOperator(Operator op) {
            if (typeRestriction == null) return true;
            return op.getClass() == typeRestriction;
        }

        public List<OperatorPlan> getSiblingsLists() {
            return siblingsLists;
        }

        private void markSeen(Iterator<Operator> operators) {
            while (operators.hasNext()) {
                seenOperators.add(operators.next());
            }
        }

        @Override
        protected void execute(LogicalRelationalOperator op) throws FrontendException {
            // Make sure we put each operator into max. one sibling group.
            if (seenOperators.contains(op)) {
                return;
            }
            seenOperators.add(op);

            // Check whether the operator matches the type of the Merging Rule.
            if (!isValidOperator(op)) {
                return;
            }

            // Find all siblings.
            OperatorSubPlan siblings = OperatorPlanUtil.findSiblings(op, true, true);
            if (siblings.size() > 1) {
                siblingsLists.add(siblings);
                markSeen(siblings.getOperators());
            }
        }
    }
}

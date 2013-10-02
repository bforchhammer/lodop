package de.uni_potsdam.hpi.loddp.optimization.rules;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rule which tries to combine filter statements if they are contained with each other.
 */
public class CombineFilters extends MergingRule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.combine-filters";

    public CombineFilters() {
        super(NAME, LOFilter.class);
    }

    @Override
    public Transformer getNewTransformer() {
        return new CombineFiltersTransformer();
    }

    protected class CombineFiltersTransformer extends SiblingTransformer {

        private Map<Operator, Set<Operator>> containments = new HashMap<Operator, Set<Operator>>();
        private OperatorSubPlan changes;

        @Override
        public boolean check(OperatorPlan plan) throws FrontendException {
            return false;
        }

        private void compare(LOFilter filter1, LOFilter filter2) {
            // Possible outcome when comparing two filters
            // 1. filter1 is completely contained within filter2
            // --> move filter1 underneith filter2
            // 2. filter1 completely contains filter2
            // --> move filter2 underneith filter1
            // 3. filter1 and filter2 overlap partially
            // --> a) do nothing
            // --> b) create new operator, move both underneith new one.
            // 4. filters are completely orthogonal; neither is contained within the other.
            // --> a) do nothing
            // --> b) create new operator, move both underneith new one.

            // todo:
            // - research containment on boolean expressions; e.g. do we need a certain normal form?
            // - e.g. (A and B) or C vs. A and (B or C) --> how do we efficiently compare them?
        }

        @Override
        public void transform(OperatorPlan plan) throws FrontendException {
            changes = new OperatorSubPlan(currentPlan);

            for (Map.Entry<Operator, Set<Operator>> pair : containments.entrySet()) {
                Operator newPredecessor = pair.getKey();
                Set<Operator> containedOperators = pair.getValue();
                for (Operator operator : containedOperators) {
                    // Remove current predecessors
                    List<Operator> predecessors = currentPlan.getPredecessors(operator);
                    for (Operator oldPredecessor : predecessors) {
                        currentPlan.disconnect(oldPredecessor, operator);
                    }

                    // Reconnect underneith the new parent
                    currentPlan.connect(newPredecessor, operator);

                    // Keep track of changed operators.
                    changes.add(operator);
                }
            }
            containments.clear();
        }

        @Override
        public OperatorPlan reportChanges() {
            return changes;
        }
    }
}

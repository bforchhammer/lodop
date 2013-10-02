package de.uni_potsdam.hpi.loddp.optimization.merging.rules;

import de.uni_potsdam.hpi.loddp.common.OperatorPlanUtil;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class MergeIdenticalOperators extends MergingRule {
    private static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.merge-identical-operators";

    public MergeIdenticalOperators() {
        super(NAME);
    }

    @Override
    public Transformer getNewTransformer() {
        return new MergeIdenticalOperatorsTransformer();
    }

    protected class MergeIdenticalOperatorsTransformer extends Transformer {

        Map<Operator, Operator> replacements = new HashMap<Operator, Operator>();

        @Override
        public boolean check(OperatorPlan plan) throws FrontendException {
            Iterator<Operator> operators = plan.getOperators();
            Iterator<Operator> candidates = plan.getOperators();
            while (operators.hasNext()) {
                Operator target = operators.next();
                while (candidates.hasNext()) {
                    Operator candidate = candidates.next();

                    // Do not replace an operator with itself.
                    if (target == candidate) continue;

                    // Do not replace an operator with one that is already being replaced by another one.
                    if (replacements.keySet().contains(candidate)) continue;

                    // If both operators are equal, add the pair to the list of replacements (with the "target"
                    // operator being replaced by the "candidate").
                    if (candidate.isEqual(target)) {
                        replacements.put(target, candidate);
                        break;
                    }
                }
            }

            return !replacements.isEmpty();
        }

        @Override
        public void transform(OperatorPlan plan) throws FrontendException {
            for (Map.Entry<Operator, Operator> pair : replacements.entrySet()) {
                OperatorPlanUtil.replace(pair.getKey(), pair.getValue());
            }
        }

        @Override
        public OperatorPlan reportChanges() {
            // @todo Check whether this is correct and necessary
            OperatorSubPlan changes = new OperatorSubPlan(currentPlan);
            Collection<Operator> remaining = replacements.values();
            for (Operator r : remaining) changes.add(r);
            return changes;
        }
    }
}

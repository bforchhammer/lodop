package de.uni_potsdam.hpi.loddp.optimization.rules;

import de.uni_potsdam.hpi.loddp.common.OperatorPlanUtil;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.*;

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

        private boolean operatorsAreEqual(Operator op1, Operator op2) {
            try {
                return op1.isEqual(op2);
            } catch (Throwable e) {
                return false;
            }
        }

        private List<Operator> getOperatorList(OperatorPlan plan) {
            Iterator<Operator> iterator = plan.getOperators();
            List<Operator> operators = new ArrayList<Operator>();
            while (iterator.hasNext()) {
                operators.add(iterator.next());
            }
            return operators;
        }

        @Override
        public boolean check(OperatorPlan plan) throws FrontendException {
            List<Operator> operators = getOperatorList(plan);
            for (int i = 0; i < operators.size(); i++) {
                // This is the pivot operator; if we find any matching operators we will replace them with this one.
                Operator pivot = operators.get(i);

                // If the pivot is already being replaced, skip it.
                if (replacements.containsKey(pivot)) {
                    continue;
                }

                for (int j = i + 1; j < operators.size(); j++) {
                    // If they are equal, replace the later with the former.
                    if (operatorsAreEqual(operators.get(i), operators.get(j))) {
                        replacements.put(operators.get(j), operators.get(i));
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
            replacements.clear();
        }

        @Override
        public OperatorPlan reportChanges() {
            return new OperatorSubPlan(currentPlan);
        }
    }
}

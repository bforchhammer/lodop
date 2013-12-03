package de.uni_potsdam.hpi.loddp.optimization.rules;

import de.uni_potsdam.hpi.loddp.common.OperatorPlanUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MergeIdenticalOperators extends MergingRule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.merge-identical-operators";
    protected static final Log log = LogFactory.getLog(MergeIdenticalOperators.class);

    public MergeIdenticalOperators() {
        super(NAME);
    }

    @Override
    public Transformer getNewTransformer() {
        return new MergeIdenticalOperatorsTransformer();
    }

    protected class MergeIdenticalOperatorsTransformer extends SiblingTransformer {

        Map<Operator, Operator> replacements = new HashMap<Operator, Operator>();

        @Override
        public boolean check(OperatorPlan plan) throws FrontendException {
            List<Operator> operators = getOperatorList(plan);
            for (int i = 0; i < operators.size(); i++) {
                // This is the pivot operator; if we find any matching operators we will replace them with this one.
                Operator pivot = operators.get(i);

                // If the current pivot operator is already being replaced by another one, then skip it.
                if (replacements.containsKey(pivot)) {
                    continue;
                }

                for (int j = i + 1; j < operators.size(); j++) {
                    // If they are equal, replace matched with the pivot operator.
                    if (operatorsAreEqual(operators.get(j), pivot)) {
                        replacements.put(operators.get(j), pivot);
                    }
                }
            }
            return !replacements.isEmpty();
        }

        private boolean operatorsAreEqual(Operator op1, Operator op2) {
            try {
                return op1.isEqual(op2);
            } catch (Throwable e) {
                return false;
            }
        }

        @Override
        public void transformPlan(OperatorPlan plan) throws FrontendException {
            int originalSize = currentPlan.size();
            for (Map.Entry<Operator, Operator> pair : replacements.entrySet()) {
                changes.add(pair.getValue());
                markSuccessorsChanged(pair.getKey());

                OperatorPlanUtil.replace(pair.getKey(), pair.getValue());
            }
            log.info("Merged identical operators: replaced " + replacements.size() + " operator(s) with identical " +
                "one(s). Plan size reduced from " + originalSize + " to " + currentPlan.size() + ".");
            replacements.clear();
        }
    }
}

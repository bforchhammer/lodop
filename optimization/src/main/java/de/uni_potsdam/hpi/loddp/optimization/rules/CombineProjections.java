package de.uni_potsdam.hpi.loddp.optimization.rules;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.optimizer.Transformer;

/**
 *
 */
public class CombineProjections extends MergingRule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.combine-projections";

    public CombineProjections() {
        super(NAME, LOForEach.class);
    }

    @Override
    public Transformer getNewTransformer() {
        return null;
    }

    protected class CombineProjectionsTransformer extends SiblingTransformer {
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            // 1. Only combine projection which have other LOForeach between this one and any leaves.
            //    Technically, those other ones would also need to be fairly restrictive; any * output would
            //    cause the output in the end to be different.
            // 2. Merge all remaining foreachs.
            return false;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            // Merge and combine all expression plans of remaining operators.
            // Replace all of them with new operator and new expression plan.
        }

        @Override
        public OperatorPlan reportChanges() {
            return null;
        }
    }
}

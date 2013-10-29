package de.uni_potsdam.hpi.loddp.optimization.rules;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public abstract class SiblingTransformer extends Transformer {
    protected OperatorPlan changes = null;

    @Override
    public final void transform(OperatorPlan matched) throws FrontendException {
        if (matched instanceof OperatorSubPlan) {
            changes = new OperatorSubPlan(((OperatorSubPlan) matched).getBasePlan());
        } else {
            changes = new OperatorSubPlan(matched);
        }
        transformPlan(matched);
    }

    public abstract void transformPlan(OperatorPlan matched) throws FrontendException;

    protected List<Operator> getOperatorList(OperatorPlan plan) {
        Iterator<Operator> iterator = plan.getOperators();
        List<Operator> operators = new ArrayList<Operator>();
        while (iterator.hasNext()) {
            operators.add(iterator.next());
        }
        return operators;
    }

    protected void markPredecessorsChanged(Operator operator) {
        List<Operator> operators = operator.getPlan().getPredecessors(operator);
        for (Operator op : operators) {
            changes.add(op);
        }
    }

    protected void markSuccessorsChanged(Operator operator) {
        List<Operator> operators = operator.getPlan().getSuccessors(operator);
        for (Operator op : operators) {
            changes.add(op);
        }
    }

    protected Operator getParentOperator(OperatorPlan siblingPlan) {
        Operator firstSibling = siblingPlan.getSinks().get(0);
        OperatorPlan realPlan = firstSibling.getPlan();
        // Because we are working on siblings that parent will be the common parent of all operators in the plan.
        return realPlan.getPredecessors(firstSibling).get(0);
    }

    @Override
    public OperatorPlan reportChanges() {
        return changes;
    }
}

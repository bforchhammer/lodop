package de.uni_potsdam.hpi.loddp.optimization.rules;

import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public abstract class SiblingTransformer extends Transformer {
    protected List<Operator> getOperatorList(OperatorPlan plan) {
        Iterator<Operator> iterator = plan.getOperators();
        List<Operator> operators = new ArrayList<Operator>();
        while (iterator.hasNext()) {
            operators.add(iterator.next());
        }
        return operators;
    }

}

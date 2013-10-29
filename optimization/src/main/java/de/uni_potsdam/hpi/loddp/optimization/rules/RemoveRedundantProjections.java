package de.uni_potsdam.hpi.loddp.optimization.rules;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.*;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Cleanup any unnecessary "simple projections" added by {@link CombineForeach}.
 */
public class RemoveRedundantProjections extends Rule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.remove-redundant-projections";

    public RemoveRedundantProjections() {
        super(NAME, false);
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator op = new LOForEach(plan);
        plan.add(op);
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new IgnoreProjectionsTransformer();
    }

    protected class IgnoreProjectionsTransformer extends SiblingTransformer {
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            Iterator<Operator> operators = matched.getOperators();

            foreachOperatorsLoop:
            while (operators.hasNext()) {
                LOForEach foreach = (LOForEach) operators.next();
                List<Operator> predecessors = currentPlan.getPredecessors(foreach);

                // Check that the ForEach operator has a parent ForEach operator.
                if (predecessors == null || predecessors.size() != 1 || !(predecessors.get(0) instanceof LOForEach)) {
                    operators.remove();
                    continue foreachOperatorsLoop;
                }

                // Check that the ForEach operator only consists of ProjectExpressions.
                LogicalPlan foreachInnerPlan = foreach.getInnerPlan();
                LOGenerate generate = (LOGenerate) foreachInnerPlan.getSinks().get(0);
                List<LogicalExpressionPlan> generateExpressions = generate.getOutputPlans();
                List<Integer> projectedColumns = new ArrayList<Integer>();
                for (LogicalExpressionPlan expression : generateExpressions) {
                    List<Operator> expressionSources = expression.getSources();
                    for (Operator expressionSource : expressionSources) {
                        if (expressionSource instanceof ProjectExpression) {
                            int inputNumber = ((ProjectExpression) expressionSource).getInputNum();
                            if (foreachInnerPlan.getSources().size() <= inputNumber) {
                                throw new RuntimeException("Faulty input number.");
                            }
                            LOInnerLoad innerLoad = (LOInnerLoad) foreachInnerPlan.getSources().get(inputNumber);
                            projectedColumns.add(innerLoad.getColNum());
                        } else {
                            operators.remove();
                            continue foreachOperatorsLoop;
                        }
                    }
                }

                // Check that the ForEach operator projects ALL expressions from its' parent ForEach operator.
                LOForEach parentForeach = (LOForEach) predecessors.get(0);
                LOGenerate parentGenerate = (LOGenerate) parentForeach.getInnerPlan().getSinks().get(0);
                List<LogicalExpressionPlan> parentExpressions = parentGenerate.getOutputPlans();
                for (int i = 0; i < parentExpressions.size(); i++) {
                    if (!projectedColumns.contains(i)) {
                        operators.remove();
                        continue foreachOperatorsLoop;
                    }
                }
            }
            return matched.size() > 0;
        }

        @Override
        public void transformPlan(OperatorPlan matched) throws FrontendException {
            Iterator<Operator> operators = matched.getOperators();
            while (operators.hasNext()) {
                currentPlan.removeAndReconnect(operators.next());
            }
        }
    }
}

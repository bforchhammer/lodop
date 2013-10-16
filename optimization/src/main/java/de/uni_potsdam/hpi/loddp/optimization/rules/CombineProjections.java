package de.uni_potsdam.hpi.loddp.optimization.rules;

import de.uni_potsdam.hpi.loddp.common.LOForEachBuilder;
import de.uni_potsdam.hpi.loddp.common.OperatorPlanUtil;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.*;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class CombineProjections extends MergingRule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.combine-projections";
    protected List<LOForEach> mergedForEachs = new ArrayList<LOForEach>();

    public CombineProjections() {
        super(NAME, LOForEach.class);
    }

    @Override
    public Transformer getNewTransformer() {
        return new CombineProjectionsTransformer();
    }

    protected class CombineProjectionsTransformer extends SiblingTransformer {
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            Iterator<Operator> operators = matched.getOperators();

            operatorLoop:
            while (operators.hasNext()) {
                LOForEach foreach = (LOForEach) operators.next();

                int predecessorsCount = currentPlan.getPredecessors(foreach).size();
                if (predecessorsCount != 1) {
                    // If plans are constructed properly then this should never happen!
                    throw new FrontendException("Found foreach with " + predecessorsCount + " predecessors instead of" +
                        " just one.");
                }

                // Check that we are not trying to merge the same siblings multiple times.
                Operator input = currentPlan.getPredecessors(foreach).get(0);
                if (mergedForEachs.contains(input)) {
                    operators.remove();
                    break operatorLoop;
                }

                // Check that the foreach has only LOGenerate and LOInnerLoad, i.e., ignore nested foreach loops.
                Iterator<Operator> it = foreach.getInnerPlan().getOperators();
                while (it.hasNext()) {
                    Operator op = it.next();
                    if (!(op instanceof LOGenerate || op instanceof LOInnerLoad)) {
                        log.debug("Skipped combination of LOForEach: inner plan contains more than just LOInnerLoad " +
                            "and LOGenerate.");
                        operators.remove();
                        break operatorLoop;
                    }
                }

                // Check that the foreach has no flatten in its generate statement.
                LOGenerate generate = (LOGenerate) foreach.getInnerPlan().getSinks().get(0);
                for (boolean flatten : generate.getFlattenFlags()) {
                    if (flatten) {
                        log.debug("Skipped combination of LOForEach: LOGenerate contains FLATTEN operators.");
                        operators.remove();
                        break operatorLoop;
                    }
                }
            }

            return matched.size() > 1;
        }

        @Override
        public void transformPlan(OperatorPlan matched) throws FrontendException {
            LOForEachBuilder mergedForEach = new LOForEachBuilder(currentPlan);
            mergedForEach.setAlias("combined projection");

            // Connect the new foreach.
            Operator parentOperator = getParentOperator(matched);
            OperatorPlanUtil.attachChild(parentOperator, mergedForEach.getForeach());

            Iterator<Operator> operators = matched.getOperators();
            while (operators.hasNext()) {
                LOForEach foreach = (LOForEach) operators.next();
                LOForEach newForeach = mergeAndAdjust(mergedForEach, foreach);

                // Move old ForEach operator underneith merged operator.
                OperatorPlanUtil.attachChild(mergedForEach.getForeach(), newForeach);

                // Replace old foreach operator with adjusted one (=copy list of successors).
                OperatorPlanUtil.replace(foreach, newForeach);
            }

            mergedForEachs.add(mergedForEach.getForeach());
            changes.add(mergedForEach.getForeach());
        }

        private LOForEach mergeAndAdjust(LOForEachBuilder mergedForeach, LOForEach foreach) throws FrontendException {
            LogicalPlan foreachInnerPlan = foreach.getInnerPlan();
            LOGenerate generate = (LOGenerate) foreachInnerPlan.getSinks().get(0);
            List<LogicalExpressionPlan> generateExpressions = generate.getOutputPlans();

            // Adjust existing LOForEach => replace each LOGenerate expression with simple projection.
            LOForEachBuilder newForeach = new LOForEachBuilder(currentPlan);
            newForeach.setAlias(foreach.getAlias() + "'");

            // Merge inner plans by merging all LOGenerate expressions. We should end up with only one generate,
            // and one LOInnerLoad for each expression. When we merge the expression plans we want to avoid
            // identities so we don't perform the same work twice.
            for (int i = 0; i < generateExpressions.size(); i++) {
                LOInnerLoad innerLoad = (LOInnerLoad) foreachInnerPlan.getSources().get(i);
                LogicalSchema userSchema = null;
                if (generate.getUserDefinedSchema() != null && generate.getUserDefinedSchema().get(i) != null) {
                    userSchema = generate.getUserDefinedSchema().get(i);
                }
                int columnId = mergedForeach.mergeGenerateExpression(innerLoad.getColNum(),
                    generateExpressions.get(i), userSchema);
                newForeach.addSimpleProjection(columnId);
            }

            return newForeach.getForeach();
        }
    }
}

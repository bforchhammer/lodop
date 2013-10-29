package de.uni_potsdam.hpi.loddp.optimization.rules;

import de.uni_potsdam.hpi.loddp.common.LOForEachBuilder;
import de.uni_potsdam.hpi.loddp.common.OperatorPlanUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.*;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.*;

/**
 * Rule which tries to combine foreach statements, if they have the same predecessor, and do not contain nested foreach
 * loops nor FLATTEN operators.
 *
 * Correctness is ensured by the addition of simple projections as children of the combined foreach operator.
 */
public class CombineForeach extends MergingRule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.combine-foreach";
    protected static final Log log = LogFactory.getLog(CombineForeach.class);
    protected static List<LOForEach> mergedForEachs = new ArrayList<LOForEach>();

    public CombineForeach() {
        super(NAME, LOForEach.class);
    }

    public static void resetCombinedList() {
        mergedForEachs = new ArrayList<LOForEach>();
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
                    continue operatorLoop;
                }

                // Check that the foreach has only LOGenerate and LOInnerLoad, i.e., ignore nested foreach loops.
                Iterator<Operator> it = foreach.getInnerPlan().getOperators();
                while (it.hasNext()) {
                    Operator op = it.next();
                    if (!(op instanceof LOGenerate || op instanceof LOInnerLoad)) {
                        log.debug("Skipped combination of LOForEach: inner plan contains more than just LOInnerLoad " +
                            "and LOGenerate.");
                        operators.remove();
                        continue operatorLoop;
                    }
                }

                // Check that the foreach has no flatten in its generate statement.
                LOGenerate generate = (LOGenerate) foreach.getInnerPlan().getSinks().get(0);
                for (boolean flatten : generate.getFlattenFlags()) {
                    if (flatten) {
                        log.debug("Skipped combination of LOForEach: LOGenerate contains FLATTEN operators.");
                        operators.remove();
                        continue operatorLoop;
                    }
                }
            }

            // We can only combine foreachs if there is at least two of them.
            return matched.size() > 1;
        }

        @Override
        public void transformPlan(OperatorPlan matched) throws FrontendException {
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("Combined ").append(matched.size()).append(" LOForEach operators:");

            LOForEachBuilder mergedForEach = new LOForEachBuilder(currentPlan);
            mergedForEach.setAlias("combined projection");

            // Connect the new foreach.
            Operator parentOperator = getParentOperator(matched);
            OperatorPlanUtil.attachChild(parentOperator, mergedForEach.getForeach());

            Iterator<Operator> operators = matched.getOperators();
            while (operators.hasNext()) {
                LOForEach foreach = (LOForEach) operators.next();
                logMessage.append(" ").append(foreach.getAlias());

                // Merge foreach into merged one and create adjusted version (=simple projection).
                LOForEach newForeach = mergeAndAdjust(mergedForEach, foreach);

                // Add adjusted old ForEach operator underneith the merged operator.
                OperatorPlanUtil.attachChild(mergedForEach.getForeach(), newForeach);

                // Replace old foreach operator with adjusted one (=copies list of successors).
                OperatorPlanUtil.replace(foreach, newForeach);
            }

            mergedForEachs.add(mergedForEach.getForeach());
            changes.add(mergedForEach.getForeach());
            markSuccessorsChanged(mergedForEach.getForeach());

            log.info(logMessage);
        }

        private LOForEach mergeAndAdjust(LOForEachBuilder mergedForeach, LOForEach foreach) throws FrontendException {
            LogicalPlan foreachInnerPlan = foreach.getInnerPlan();
            LOGenerate generate = (LOGenerate) foreachInnerPlan.getSinks().get(0);
            List<LogicalExpressionPlan> generateExpressions = generate.getOutputPlans();

            // Adjust existing LOForEach => replace each LOGenerate expression with simple projection.
            LOForEachBuilder newForeach = new LOForEachBuilder(currentPlan);
            newForeach.setAlias(foreach.getAlias() + "'");

            // Merge inner plans by merging all LOGenerate expressions. We should end up with only one generate. When
            // we merge the expression plans we want to avoid identities so we don't perform the same work twice.
            for (int i = 0; i < generateExpressions.size(); i++) {
                // The inputs of each expression is determined by the leaves of the expression plan; each leaf should
                // be an LOProject operator referencing LOInnerLoad inputs. In order to properly merge the
                // expression we need to find and also merge all respective LOInnerLoad operators.
                List<Operator> expressionSinks = generateExpressions.get(i).getSinks();
                Map<Integer, Integer> inputColumnNumberMap = new HashMap<Integer, Integer>();
                for (int j = 0; j < expressionSinks.size(); j++) {
                    Operator op = expressionSinks.get(j);
                    if (op instanceof ProjectExpression) {
                        int inputNumber = ((ProjectExpression) op).getInputNum();
                        if (foreachInnerPlan.getSources().size() <= inputNumber) {
                            // Something is not right.
                            throw new RuntimeException("Faulty input number.");
                        }
                        LOInnerLoad innerLoad = (LOInnerLoad) foreachInnerPlan.getSources().get(inputNumber);
                        inputColumnNumberMap.put(inputNumber, innerLoad.getColNum());
                    } else {
                        throw new FrontendException("Expression plan has a leaf operator which is not a projection");
                    }
                }

                LogicalSchema userSchema = null;
                if (generate.getUserDefinedSchema() != null && generate.getUserDefinedSchema().get(i) != null) {
                    userSchema = generate.getUserDefinedSchema().get(i);
                }
                int columnId = mergedForeach.addGenerateExpression(inputColumnNumberMap,
                    generateExpressions.get(i).deepCopy(), userSchema);
                newForeach.addSimpleProjection(columnId);
            }

            return newForeach.getForeach();
        }
    }
}

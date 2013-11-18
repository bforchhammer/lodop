package de.uni_potsdam.hpi.loddp.optimization.rules;

import de.uni_potsdam.hpi.loddp.common.printing.LogicalPlanPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.*;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.*;

/**
 * This rule removes projections, which can be removed without changing the output of any STORE leaves.
 *
 * This can help combine more child operators, if projections in question have the same parent and semantically
 * identical child operators.
 *
 * An operator is considered to be save to remove, if there is at least one other ForEach operator between the current
 * operator and all STORE leaves. See {@link IgnoreProjectionsTransformer::check()} for details and notes.
 */
public class IgnoreProjections extends Rule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.ignore-projections";
    protected static final Log log = LogFactory.getLog(IgnoreProjections.class);
    private static List<Operator> visitedOperators = new ArrayList<Operator>();

    public IgnoreProjections() {
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
        /**
         * Checks whether a ForEach operator can be removed.
         *
         * An operator is considered to be save to remove, if there is at least one other ForEach operator between the
         * current operator and all STORE leaves.
         *
         * Notes:
         *
         * 1. We currently assume that those other foreach operators have a fully specified projection list and don't
         * simply output the full input (*).
         *
         * 2. We also assume that any operators using the output of merged projections reference aliases directly, and
         * thereby do not depend on the order of operators (e.g. no usage of $1-style variables).
         */
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            Iterator<Operator> operators = matched.getOperators();

            foreachOperatorsLoop:
            while (operators.hasNext()) {
                LOForEach foreach = (LOForEach) operators.next();

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

                if (!hasForeEachSuccessors(foreach)) {
                    operators.remove();
                    continue foreachOperatorsLoop;
                }

                if (visitedOperators.contains(foreach)) {
                    operators.remove();
                    continue foreachOperatorsLoop;
                }
                visitedOperators.add(foreach);
            }

            return matched.size() > 0;
        }

        /**
         * Checks if the given operator has at least one LOForeach operator on each subtree path leading to a STORE
         * leaf.
         *
         * @param operator
         *
         * @return
         */
        private boolean hasForeEachSuccessors(Operator operator) {
            OperatorPlan plan = operator.getPlan();
            List<Operator> successors = plan.getSuccessors(operator);

            // No successors at all => return false.
            if (successors == null || successors.isEmpty()) {
                return false;
            }

            // Else check all successors.
            boolean foundAll = true;
            for (Operator successor : successors) {
                // If the successor is an operator which depends on the correct projection, then stop.
                if (successor instanceof LODistinct || successor instanceof LOStore || successor instanceof LOUnion) {
                    foundAll = false;
                    break;
                }

                // If either the current successor is a foreach, or the successor itself has a foreach successors,
                // then we are good. otherwise we just don't have one.
                else if (!(successor instanceof LOForEach) && !hasForeEachSuccessors(successor)) {
                    foundAll = false;
                    break; // no need to continue looking at other paths.
                }
            }
            return foundAll;
        }

        @Override
        public void transformPlan(OperatorPlan matched) throws FrontendException {
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("Matched " + matched.size() + " operators:");
            Iterator<Operator> operators = matched.getOperators();
            while (operators.hasNext()) {
                LOForEach foreach = (LOForEach) operators.next();

                foreach.annotate(LogicalPlanPrinter.ANNOTATION_OPERATOR_HIGHLIGHT, "#a4c2f4");
                logMessage.append(" ").append(foreach.getAlias());

                // Build projection map, which tells us which input column is projected by each expression in the op.
                Map<Integer, Integer> projectionMap = getProjections(foreach);
                for (Map.Entry<Integer, Integer> entry : projectionMap.entrySet()) {
                    logMessage.append("\n\t")
                        .append("expression ").append(entry.getKey())
                        .append(" = column ").append(entry.getValue());
                }

                fixColumnMapping(currentPlan.getSuccessors(foreach), projectionMap);

                /*markPredecessorsChanged(operator);
                markSuccessorsChanged(operator);
                currentPlan.removeAndReconnect(operators.next());*/
            }
            log.info(logMessage);
        }

        private void fixColumnMapping(List<Operator> operators, Map<Integer, Integer> projectionMap) throws FrontendException {
            for (Operator operator : operators) {
                fixColumnMapping(operator, projectionMap);
            }
        }

        private void fixColumnMapping(Operator operator, Map<Integer, Integer> projectionMap) throws FrontendException {
            log.info("Fix projection mapping on " + operator.getName());
            if (operator instanceof LOJoin) {
                // 1) Look at logical expression plans. Fix any references to the input matching the foreach operator.
                // (=only one of the 2 join inputs).
                // 2) Then look at any children referencing the "foreach" part of the output schema.
            }
            if (operator instanceof LOCogroup) {
                // 1) Look at group key (=logical expression plan) + fix any references.
                // 2) Look at children referencing the group contents.
            }
            if (operator instanceof LOForEach) {
                // 1) Look at all LOInnerLoad + fix if they reference original foreach; if this is following a
                // LOCoGroup, then there may be a Dereference operator in-between. Hmmm.
                // 2) Stop looking, because now we have a new column schema and don't need to worry anymore.
            }

            // Idea: search for schema UIDs matching the original schema UID + make sure they reference correctly?
        }

        private Map<Integer, Integer> getProjections(LOForEach foreach) throws FrontendException {
            LogicalPlan foreachInnerPlan = foreach.getInnerPlan();
            LOGenerate generate = (LOGenerate) foreachInnerPlan.getSinks().get(0);
            List<LogicalExpressionPlan> generateExpressions = generate.getOutputPlans();

            Map<Integer, Integer> projectionMap = new HashMap<Integer, Integer>();
            for (int expressionNumber = 0; expressionNumber < generateExpressions.size(); expressionNumber++) {
                List<Operator> expressionSinks = generateExpressions.get(expressionNumber).getSinks();
                if (expressionSinks.size() != 1) {
                    throw new FrontendException("Expected expression with only one leaf, " +
                        "got " + expressionSinks.size() + " instead. Not a simple projection?");
                }
                Operator op = expressionSinks.get(0);
                if (op instanceof ProjectExpression) {
                    int inputNumber = ((ProjectExpression) op).getInputNum();
                    if (foreachInnerPlan.getSources().size() <= inputNumber) {
                        throw new FrontendException("Faulty input number.");
                    }
                    LOInnerLoad innerLoad = (LOInnerLoad) foreachInnerPlan.getSources().get(inputNumber);
                    projectionMap.put(expressionNumber, innerLoad.getColNum());
                } else {
                    throw new FrontendException("Expression plan has a leaf operator which is not a projection");
                }
            }

            return projectionMap;
        }
    }

}

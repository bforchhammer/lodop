package de.uni_potsdam.hpi.loddp.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.*;

import java.util.*;

/**
 * Class for constructing LOForEach operators.
 */
public class LOForEachBuilder {
    protected static final Log log = LogFactory.getLog(LOForEachBuilder.class);
    private LOForEach foreach;
    private LogicalPlan foreachInnerPlan;
    private LOGenerate generate;

    public LOForEachBuilder(OperatorPlan plan) {
        foreach = new LOForEach(plan);

        // Setup new inner plan.
        foreach.setInnerPlan(new LogicalPlan());
        foreachInnerPlan = foreach.getInnerPlan();

        // Add a new (empty) LOGenerate.
        generate = new LOGenerate(foreachInnerPlan, new ArrayList<LogicalExpressionPlan>(), new boolean[0]);
        foreachInnerPlan.add(generate);
    }

    public void setAlias(String alias) {
        foreach.setAlias(alias);
    }

    /**
     * Add and connect an LOInnerLoad operator, if it doesn't exist already. Return the number of the input, i.e. the
     * number of the load in the list of plan sources.
     *
     * @param load
     *
     * @return
     */
    protected int addInnerLoad(LOInnerLoad load) {
        // Check if there is already a matching LOInnerLoad operator; if yes return its' input position.
        List<Operator> sources = foreachInnerPlan.getSources();
        if (sources != null) {
            for (int i = 0; i < sources.size(); i++) {
                if (operatorsAreEqual(sources.get(i), load)) {
                    return i;
                }
            }
        }

        // No matching input found, then add and connect the given load operator and return the new input number.
        foreachInnerPlan.add(load);
        foreachInnerPlan.connect(load, generate);
        return foreachInnerPlan.getSources().size() - 1;
    }

    protected boolean operatorsAreEqual(Operator op1, Operator op2) {
        try {
            return op1.isEqual(op2);
        } catch (Throwable e) {
            log.error("Failed to compare operators.", e);
        }
        return false;
    }

    protected boolean expressionsAreEqual(LogicalExpressionPlan plan1, LogicalExpressionPlan plan2) {
        try {
            return plan1.isEqual(plan2);
        } catch (FrontendException e) {
            log.error("Failed to compare expression plans.", e);
        }
        return false;
    }

    /**
     * @param innerLoads A list mapping old "input numbers" to new LOInnerLoad operators. All of these operators are
     *                   added and connected in the new inner plan. The old input number is used to correctly update any
     *                   ProjectExpression in the given logical expression.
     * @param expression The logical expression to add to the expression plan.
     * @param userSchema A user defined schema for the given expression; can be NULL.
     *
     * @return
     */
    protected int _addGenerateExpression(Map<Integer, LOInnerLoad> innerLoads, LogicalExpressionPlan expression,
                                         LogicalSchema userSchema) {

        // Map of old input numbers to new ones.
        Map<Integer, Integer> inputNumberMap = new HashMap<Integer, Integer>();

        // Attach inputs.
        for (Map.Entry<Integer, LOInnerLoad> entry : innerLoads.entrySet()) {
            int oldInputNumber = entry.getKey();
            if (inputNumberMap.containsKey(oldInputNumber)) {
                // Should never happen.
                throw new RuntimeException("Multiple LOInnerLoad operators for the same old input found.");
            }

            inputNumberMap.put(oldInputNumber, addInnerLoad(entry.getValue()));
        }

        // Make sure that ProjectExpressions contain valid references.
        Iterator<Operator> expressions = expression.getOperators();
        while (expressions.hasNext()) {
            Operator operator = expressions.next();
            if (operator instanceof ProjectExpression) {
                ((ProjectExpression) operator).setAttachedRelationalOp(generate);
                int oldInputNumber = ((ProjectExpression) operator).getInputNum();
                if (inputNumberMap.containsKey(oldInputNumber)) {
                    ((ProjectExpression) operator).setInputNum(inputNumberMap.get(oldInputNumber));
                } else {
                    // Should never happen.
                    throw new RuntimeException("Cannot map old input number to new input.");
                }
            }
        }

        // Check if equivalent expression exists already; in case of success return number of existing expression.
        List<LogicalExpressionPlan> plans = generate.getOutputPlans();
        for (int i = 0; i < plans.size(); i++) {
            if (expressionsAreEqual(plans.get(i), expression)) {
                return i;
            }
        }

        // Expression does not exist yet, so let's add it. We also need to fix up the user defined schema and flatten
        // flag on the generate operator (see below).
        int expressionNumber = plans.size();
        plans.add(expression);

        // Set user defined schema if provided.
        if (userSchema != null) {
            // If there is none set yet, we need to create a new list and fill it up with nulls for all previous
            // expressions (which evidently do not have a user defined schema).
            if (generate.getUserDefinedSchema() == null) {
                generate.setUserDefinedSchema(new ArrayList<LogicalSchema>());
                for (int i = 0; i < expressionNumber; i++) generate.getUserDefinedSchema().add(null);
            }
            // Then we can add the new "user defined schema".
            generate.getUserDefinedSchema().add(userSchema);
        }
        // If some previous expressions had a user defined schema, we should keep the list filled for easy
        // inserts if we get another expression with a user-defined schema later.
        else if (generate.getUserDefinedSchema() != null) {
            generate.getUserDefinedSchema().add(null);
        }

        // Update flatten flags, we assume all false (see check() method above).
        generate.setFlattenFlags(new boolean[plans.size()]);

        // Return column number of added expression.
        return expressionNumber;
    }

    public int addGenerateExpression(Map<Integer, Integer> inputColumnNumberMap, LogicalExpressionPlan expression, LogicalSchema userSchema) {
        Map<Integer, LOInnerLoad> loads = new HashMap<Integer, LOInnerLoad>();
        for (Map.Entry<Integer, Integer> entry : inputColumnNumberMap.entrySet()) {
            loads.put(entry.getKey(), new LOInnerLoad(foreachInnerPlan, foreach, entry.getValue()));
        }
        return _addGenerateExpression(loads, expression, userSchema);
    }

    public int addSimpleProjection(int columnId) {
        // Build Projection expression.
        LogicalExpressionPlan expression = new LogicalExpressionPlan();

        int inputNumber = 0;
        new ProjectExpression(expression, inputNumber, -1, generate);

        Map<Integer, Integer> inputColumnNumberMap = new HashMap<Integer, Integer>();
        inputColumnNumberMap.put(inputNumber, columnId);

        return addGenerateExpression(inputColumnNumberMap, expression, null);
    }

    public LOForEach getForeach() {
        return foreach;
    }
}

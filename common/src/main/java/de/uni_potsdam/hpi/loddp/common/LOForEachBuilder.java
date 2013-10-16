package de.uni_potsdam.hpi.loddp.common;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class for constructing LOForEach operators.
 */
public class LOForEachBuilder {
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

    public int addGenerateExpression(LOInnerLoad innerLoad, LogicalExpressionPlan expression) {
        return addGenerateExpression(innerLoad, expression, null);
    }

    public int addGenerateExpression(LOInnerLoad innerLoad, LogicalExpressionPlan expression,
                                     LogicalSchema userSchema) {
        // Attach input.
        foreachInnerPlan.add(innerLoad);
        foreachInnerPlan.connect(innerLoad, generate);

        // Add expression plan.
        List<LogicalExpressionPlan> plans = generate.getOutputPlans();
        int expressionNumber = plans.size();
        plans.add(expression);

        // Make sure that ProjectExpressions contain valid references.
        Iterator<Operator> expressions = expression.getOperators();
        while (expressions.hasNext()) {
            Operator operator = expressions.next();
            if (operator instanceof ProjectExpression) {
                ((ProjectExpression) operator).setAttachedRelationalOp(generate);
                ((ProjectExpression) operator).setInputNum(expressionNumber);
            }
        }

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

    public int addGenerateExpression(int columnId, LogicalExpressionPlan expression) {
        return addGenerateExpression(columnId, expression, null);
    }

    public int addGenerateExpression(int columnId, LogicalExpressionPlan expression, LogicalSchema userSchema) {
        LOInnerLoad load = new LOInnerLoad(foreachInnerPlan, foreach, columnId);
        return addGenerateExpression(load, expression, userSchema);
    }

    public int addSimpleProjection(int columnId) {
        // Build Projection expression.
        LogicalExpressionPlan expression = new LogicalExpressionPlan();
        new ProjectExpression(expression, 0, -1, generate);

        return addGenerateExpression(columnId, expression);
    }

    public int mergeGenerateExpression(int columnId, LogicalExpressionPlan expression,
                                       LogicalSchema userSchema) throws FrontendException {
        List<LogicalExpressionPlan> plans = generate.getOutputPlans();
        for (int i = 0; i < plans.size(); i++) {
            if (plans.get(i).isEqual(expression)) {
                return i;
            }
        }
        return addGenerateExpression(columnId, expression, userSchema);
    }

    public LOForEach getForeach() {
        return foreach;
    }
}

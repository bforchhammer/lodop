package de.uni_potsdam.hpi.loddp.optimization.rules;

import de.uni_potsdam.hpi.loddp.common.OperatorPlanUtil;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.LogicalExpressionSimplifier;
import org.apache.pig.newplan.optimizer.Transformer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Rule which tries to combine filter statements if they are contained with each other.
 */
public class CombineFilters extends MergingRule {
    public static final String NAME = "de.uni_potsdam.hpi.loddp.optimization.combine-filters";
    protected List<LOFilter> mergedFilters = new ArrayList<LOFilter>();

    public CombineFilters() {
        super(NAME, LOFilter.class);
    }

    @Override
    public Transformer getNewTransformer() {
        return new CombineFiltersTransformer();
    }

    protected class CombineFiltersTransformer extends SiblingTransformer {

        @Override
        public boolean check(OperatorPlan plan) throws FrontendException {
            // Make sure not to merge any filters that have been merged into a combined, parent filter already.
            Iterator<Operator> operators = plan.getOperators();
            while (operators.hasNext()) {
                LOFilter filter = (LOFilter) operators.next();
                Operator filterInput = filter.getInput((LogicalPlan) currentPlan);
                if (mergedFilters.contains(filterInput)) {
                    operators.remove();
                }
            }
            // We can only combine filters if there is at least two of them.
            return plan.size() > 1;
        }

        @Override
        public void transformPlan(OperatorPlan plan) throws FrontendException {
            LogicalPlan logicalPlan = (LogicalPlan) currentPlan;

            // Create merged filter as new child to common sibling parent.
            LOFilter combinedFilter = new LOFilter(logicalPlan);
            combinedFilter.setAlias("Combined");
            mergedFilters.add(combinedFilter);
            changes.add(combinedFilter);

            Iterator<Operator> operators = plan.getOperators();
            while (operators.hasNext()) {
                LOFilter filter = (LOFilter) operators.next();
                currentPlan.connect(filter.getInput(logicalPlan), combinedFilter);
                combineFilterConditions(combinedFilter, filter);
            }

            // Cleanup combined filter (remove redundant statements etc.).
            LogicalExpressionSimplifier simplifier = new LogicalExpressionSimplifier(NAME + "-expressions-simplifier");
            OperatorPlan subPlan = new OperatorSubPlan(currentPlan);
            subPlan.add(combinedFilter);
            Transformer transformer = simplifier.getNewTransformer();
            if (transformer.check(subPlan)) {
                transformer.transform(subPlan);
            }

            // For each sibling: if sibling == merged filter then remove sibling and connect sibling children to
            // merged filter; else connect sibling itself to merged filter.
            operators = plan.getOperators();
            while (operators.hasNext()) {
                LOFilter filter = (LOFilter) operators.next();
                if (filter.isEqual(combinedFilter)) {
                    OperatorPlanUtil.replace(filter, combinedFilter);
                    combinedFilter.setAlias(filter.getAlias());
                } else {
                    OperatorPlanUtil.insertBetween(filter.getInput(logicalPlan), combinedFilter, filter);
                    changes.add(filter);
                }
            }

            // Fix potentially broken references in ProjectExpressions.
            Iterator<Operator> expressions = combinedFilter.getFilterPlan().getOperators();
            while (expressions.hasNext()) {
                Operator expression = expressions.next();
                if (expression instanceof ProjectExpression) {
                    ((ProjectExpression) expression).setAttachedRelationalOp(combinedFilter);
                }
            }
        }

        // combine the condition of two filters. The condition of second filter
        // is added into the condition of first filter with an OR operator.
        private void combineFilterConditions(LOFilter targetFilter, LOFilter otherFilter) throws FrontendException {
            LogicalExpressionPlan p1 = targetFilter.getFilterPlan();
            // Create a new logical expression plan for the targetFilter if it does not have one yet.
            if (p1 == null) {
                p1 = new LogicalExpressionPlan();
                targetFilter.setFilterPlan(p1);
            }
            LogicalExpressionPlan p2 = otherFilter.getFilterPlan().deepCopy();
            p1.merge(p2);

            // Combine sources as OR expressions.
            List<Operator> sources = p1.getSources();
            if (sources.size() == 1) {
                // we probably just created the merged plan so nothing to combine yet.
            } else if (sources.size() == 2) {
                // We should not have two sources for combination.
                new OrExpression(p1, (LogicalExpression) sources.get(0), (LogicalExpression) sources.get(1));
            } else {
                // This should not happen. Did we miss something?
                throw new FrontendException("Expected only two sources after combining filters, " +
                    "got " + sources.size() + " instead.");
            }
        }
    }
}

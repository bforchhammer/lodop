package de.uni_potsdam.hpi.loddp.optimization;

import de.uni_potsdam.hpi.loddp.optimization.rules.CombineFilter;
import de.uni_potsdam.hpi.loddp.optimization.rules.CombineForeach;
import de.uni_potsdam.hpi.loddp.optimization.rules.MergeIdenticalOperators;
import de.uni_potsdam.hpi.loddp.optimization.rules.RemoveRedundantProjections;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.optimizer.PlanOptimizer;

/**
 * Build a custom plan optimizer instance.
 */
public class PlanOptimizerBuilder {

    private boolean combineFilters;
    private boolean combineForeachs;
    private boolean ignoreProjections;

    public PlanOptimizerBuilder() {
        this(true);
    }

    public PlanOptimizerBuilder(boolean defaultValue) {
        combineFilters = defaultValue;
        combineForeachs = defaultValue;
        ignoreProjections = defaultValue;
    }

    /**
     * Returns a custom plan optimizer instance which simply applies ALL available rules.
     */
    public static PlanOptimizer getDefaultInstance(LogicalPlan plan) {
        PlanOptimizerBuilder builder = new PlanOptimizerBuilder();
        return builder.getInstance(plan);
    }

    public void setCombineFilters(boolean combineFilters) {
        this.combineFilters = combineFilters;
    }

    public void setCombineForeachs(boolean combineForeachs) {
        this.combineForeachs = combineForeachs;
    }

    public void setIgnoreProjections(boolean ignoreProjections) {
        this.ignoreProjections = ignoreProjections;
    }

    /**
     * Returns a custom plan optimizer instance.
     */
    public PlanOptimizer getInstance(LogicalPlan plan) {
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(plan);

        if (combineFilters) {
            optimizer.addRuleSet(new CombineFilter());
        }

        if (combineForeachs) {
            optimizer.addRuleSet(new CombineForeach(), new RemoveRedundantProjections());
        }

        if (ignoreProjections) {
            //optimizer.addRuleSet(new IgnoreProjections());
        }

        if (combineFilters || combineForeachs || ignoreProjections) {
            // Repeat "identical merge" rules?
            optimizer.addRuleSet(new MergeIdenticalOperators());
        }

        return optimizer;
    }
}

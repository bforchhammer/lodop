package de.uni_potsdam.hpi.loddp.optimization;

import de.uni_potsdam.hpi.loddp.optimization.rules.CombineFilter;
import de.uni_potsdam.hpi.loddp.optimization.rules.CombineForeach;
import de.uni_potsdam.hpi.loddp.optimization.rules.MergeIdenticalOperators;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.ProjectionPatcher;
import org.apache.pig.newplan.logical.optimizer.SchemaPatcher;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class LogicalPlanOptimizer extends PlanOptimizer {
    public LogicalPlanOptimizer(OperatorPlan p) {
        super(p, null, 500);
        ruleSets = buildRuleSets();
        addListeners();
    }

    protected List<Set<Rule>> buildRuleSets() {
        List<Set<Rule>> rules = new ArrayList<Set<Rule>>();

        Set<Rule> mergeIdentical = new HashSet<Rule>();
        mergeIdentical.add(new MergeIdenticalOperators());
        rules.add(mergeIdentical);

        Set<Rule> combineFilters = new HashSet<Rule>();
        //combineFilters.add(new LogicalExpressionSimplifier("FilterLogicExpressionSimplifier"));
        combineFilters.add(new CombineFilter());
        rules.add(combineFilters);

        Set<Rule> combineProjections = new HashSet<Rule>();
        combineProjections.add(new CombineForeach());
        //combineProjections.add(new IgnoreProjections());
        rules.add(combineProjections);

        // Repeat "identical merge" rules.
        /*rules.add(mergeIdentical);*/

        return rules;
    }

    private void addListeners() {
        addPlanTransformListener(new SchemaPatcher());
        addPlanTransformListener(new ProjectionPatcher());
    }
}

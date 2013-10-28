package de.uni_potsdam.hpi.loddp.optimization;

import de.uni_potsdam.hpi.loddp.optimization.rules.MergeIdenticalOperators;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.ProjectionPatcher;
import org.apache.pig.newplan.logical.optimizer.SchemaPatcher;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 */
public class LogicalPlanOptimizer extends PlanOptimizer {

    public LogicalPlanOptimizer(OperatorPlan p) {
        super(p, null, 500);

        // Initialise rule sets, add basic merging rule.
        ruleSets = new ArrayList<Set<Rule>>();
        addRule(new MergeIdenticalOperators());

        // Add listeners for plan changes.
        addListeners();
    }

    public void addRule(Rule rule) {
        Set<Rule> ruleSet = new HashSet<Rule>();
        ruleSet.add(rule);
        ruleSets.add(ruleSet);
    }

    private void addListeners() {
        addPlanTransformListener(new SchemaPatcher());
        addPlanTransformListener(new ProjectionPatcher());
    }
}

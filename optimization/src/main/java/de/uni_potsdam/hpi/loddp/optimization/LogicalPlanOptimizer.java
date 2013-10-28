package de.uni_potsdam.hpi.loddp.optimization;

import de.uni_potsdam.hpi.loddp.optimization.rules.CombineFilter;
import de.uni_potsdam.hpi.loddp.optimization.rules.CombineForeach;
import de.uni_potsdam.hpi.loddp.optimization.rules.MergeIdenticalOperators;
import org.apache.pig.impl.logicalLayer.FrontendException;
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

    private boolean combineFilters = true;
    private boolean combineForeachs = true;
    private boolean ignoreProjections = true;

    public LogicalPlanOptimizer(OperatorPlan p) {
        super(p, null, 500);
        addListeners();
    }

    @Override
    public void optimize() throws FrontendException {
        ruleSets = buildRuleSets();
        super.optimize();
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

    protected List<Set<Rule>> buildRuleSets() {
        List<Set<Rule>> rules = new ArrayList<Set<Rule>>();

        Set<Rule> mergeIdentical = new HashSet<Rule>();
        mergeIdentical.add(new MergeIdenticalOperators());
        rules.add(mergeIdentical);

        if (combineFilters) {
            Set<Rule> combineFilters = new HashSet<Rule>();
            combineFilters.add(new CombineFilter());
            rules.add(combineFilters);
        }

        if (combineForeachs) {
            Set<Rule> combineForeach = new HashSet<Rule>();
            combineForeach.add(new CombineForeach());
            rules.add(combineForeach);
        }

        if (ignoreProjections) {
            Set<Rule> ignoreProjections = new HashSet<Rule>();
            //ignoreProjections.add(new IgnoreProjections());
            rules.add(ignoreProjections);
        }

        if (combineFilters || combineForeachs || ignoreProjections) {
            // Repeat "identical merge" rules?
            rules.add(mergeIdentical);
        }

        return rules;
    }

    private void addListeners() {
        addPlanTransformListener(new SchemaPatcher());
        addPlanTransformListener(new ProjectionPatcher());
    }
}

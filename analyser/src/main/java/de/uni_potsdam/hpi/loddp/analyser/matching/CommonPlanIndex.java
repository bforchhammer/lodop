package de.uni_potsdam.hpi.loddp.analyser.matching;

import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScript;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import java.util.*;

public class CommonPlanIndex extends TreeMap<OperatorSubPlan, Set<AnalysedScript>> {

    public CommonPlanIndex() {
        super(new PlanComparator());
    }

    public void add(Set<OperatorSubPlan> plans, AnalysedScript s1, AnalysedScript s2) {
        for (OperatorSubPlan plan : plans) {
            add(plan, s1, s2);
        }
    }

    public void add(OperatorSubPlan plan, AnalysedScript s1, AnalysedScript s2) {
        Set<AnalysedScript> scripts;
        if (super.containsKey(plan)) {
            scripts = super.get(plan);
        } else {
            scripts = new HashSet<AnalysedScript>();
            super.put(plan, scripts);
        }
        scripts.add(s1);
        scripts.add(s2);
    }

    /**
     * Remove un-interesting plans, e.g. plans consist of only one operator.
     */
    public void prune() {
        removeSmallPlans();
    }

    private void removeSmallPlans() {
        List<OperatorSubPlan> remove = new LinkedList<OperatorSubPlan>();
        for (OperatorSubPlan plan : super.keySet()) {
            if (plan.size() == 1) {
                remove.add(plan);
            }
        }
        for (OperatorSubPlan plan : remove) {
            super.remove(plan);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<OperatorSubPlan, Set<AnalysedScript>> entry : super.entrySet()) {
            OperatorSubPlan plan = entry.getKey();
            sb.append("Plan [");
            sb.append(plan.size()).append(" ops, ");
            sb.append(entry.getValue().size()).append(" scripts]: ");

            List<Operator> operators = plan.getSources();
            int depth = 0;
            while (operators != null && operators.size() > 0) {
                if (depth++ > 0) sb.append(" > ");
                Operator operator = operators.get(0);
                if (operator instanceof LogicalRelationalOperator) {
                    sb.append("").append(((LogicalRelationalOperator) operator).getAlias()).append(":");
                }
                sb.append(operator.getName());
                operators = plan.getSuccessors(operator);
            }

            for (AnalysedScript script : entry.getValue()) {
                sb.append("\n\t - ").append(script.getScriptName());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Simple comparator which orders OperatorSubPlan instance by their size, and
     */
    private static class PlanComparator implements Comparator<OperatorSubPlan> {
        @Override
        public int compare(OperatorSubPlan o1, OperatorSubPlan o2) {
            try {
                if (o1.isEqual(o2)) return 0;
            } catch (FrontendException e) {
                e.printStackTrace();
            }

            // Establish an arbitrary order via different properties of the two plans; The only important thing is
            // to avoid returning 0 which represents equality. @todo use a hash-based implementation instead (?).


            // Prefer order by plan size
            int order = o1.size() - o2.size();
            if (order != 0) return order;

            // Fallback to difference between hashcodes (= essentially random).
            return o1.hashCode() - o2.hashCode();
        }
    }
}

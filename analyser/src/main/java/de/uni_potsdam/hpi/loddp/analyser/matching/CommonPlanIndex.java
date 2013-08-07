package de.uni_potsdam.hpi.loddp.analyser.matching;

import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import java.util.*;

public class CommonPlanIndex extends HashMap<SubPlan, Set<AnalysedScript>> {

    protected static final Log log = LogFactory.getLog(CommonPlanIndex.class);

    public void add(Set<SubPlan> plans, AnalysedScript s1, AnalysedScript s2) {
        for (SubPlan plan : plans) {
            add(plan, s1, s2);
        }
    }

    public void add(SubPlan plan, AnalysedScript s1, AnalysedScript s2) {
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
        Iterator<SubPlan> it = keySet().iterator();
        while (it.hasNext()) {
            if (it.next().size() == 1) it.remove();
        }
    }

    @Override
    public String toString() {
        return toString(super.entrySet());
    }

    protected String toString(Comparator<Map.Entry<SubPlan, Set<AnalysedScript>>> comparator) {
        ArrayList<Map.Entry<SubPlan, Set<AnalysedScript>>> sortedList = new ArrayList<Map.Entry<SubPlan, Set<AnalysedScript>>>(super.entrySet());
        Collections.sort(sortedList, Collections.reverseOrder(comparator));
        return toString(sortedList);
    }

    public String toStringSortByPlanSize() {
        return toString(new PlanSizeComparator());
    }

    public String toStringSortByNumberOfScripts() {
        return toString(new ScriptNumberComparator());
    }

    private String toString(Collection<Map.Entry<SubPlan, Set<AnalysedScript>>> entrySet) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<SubPlan, Set<AnalysedScript>> entry : entrySet) {
            SubPlan plan = entry.getKey();
            sb.append("Plan [");
            sb.append(plan.size()).append(" ops, ");
            sb.append(entry.getValue().size()).append(" scripts]: ");

            planToString(plan, sb);

            for (AnalysedScript script : entry.getValue()) {
                sb.append("\n\t - ").append(script.getScriptName());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private void planToString(SubPlan plan, StringBuilder sb) {
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
    }

    public static class PlanSizeComparator implements Comparator<Map.Entry<SubPlan, Set<AnalysedScript>>> {
        @Override
        public int compare(Map.Entry<SubPlan, Set<AnalysedScript>> o1, Map.Entry<SubPlan, Set<AnalysedScript>> o2) {
            return o1.getKey().size() - o2.getKey().size();
        }
    }

    public static class ScriptNumberComparator implements Comparator<Map.Entry<SubPlan, Set<AnalysedScript>>> {
        @Override
        public int compare(Map.Entry<SubPlan, Set<AnalysedScript>> o1, Map.Entry<SubPlan, Set<AnalysedScript>> o2) {
            return o1.getValue().size() - o2.getValue().size();
        }
    }

}

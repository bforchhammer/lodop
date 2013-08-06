package de.uni_potsdam.hpi.loddp.analyser.matching;

import de.uni_potsdam.hpi.loddp.analyser.script.AnalysedScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import java.util.*;

/**
 * Matches two logical plans and determines e.g. common pre-processing steps (=overlapping list of operators starting
 * from plan sources).
 */
public class LogicalPlanMatcher {

    protected static final Log log = LogFactory.getLog(LogicalPlanMatcher.class);
    private LogicalPlan plan1;
    private LogicalPlan plan2;

    public LogicalPlanMatcher(LogicalPlan plan1, LogicalPlan plan2) {
        this.plan1 = plan1;
        this.plan2 = plan2;
    }

    public static void findCommonPreprocessing(List<AnalysedScript> scripts) {
        findCommonPreprocessing(scripts, true);
    }

    /**
     * Determines all possible pairs within the given set of scripts, then compares the logical plans of each pair of
     * scripts. The result is printed to the INFO log.
     */
    public static void findCommonPreprocessing(List<AnalysedScript> scripts, boolean useOptimized) {
        CommonPlanIndex commonPPIndex = new CommonPlanIndex();

        ListIterator<AnalysedScript> iterator1 = scripts.listIterator();
        ListIterator<AnalysedScript> iterator2;

        while (iterator1.hasNext()) {
            AnalysedScript s1 = iterator1.next();
            if (!iterator1.hasNext()) {
                // We have reached the end of the list.
                break;
            }
            iterator2 = scripts.listIterator(iterator1.nextIndex());
            while (iterator2.hasNext()) {
                AnalysedScript s2 = iterator2.next();
                LogicalPlanMatcher matcher = new LogicalPlanMatcher(
                    useOptimized ? s1.getLogicalPlan() : s1.getUnoptimizedLogicalPlan(),
                    useOptimized ? s2.getLogicalPlan() : s2.getUnoptimizedLogicalPlan());
                Set<OperatorSubPlan> common = matcher.findCommonPreprocessing();
                printCommonPlans(common, s1, s2);
                commonPPIndex.add(common, s1, s2);
            }
        }
        commonPPIndex.prune();

        log.info("Common plan index (sorted by plan size): \n" + commonPPIndex.toStringSortByPlanSize());
        log.info("Common plan index (sorted by number of scripts): \n" + commonPPIndex.toStringSortByNumberOfScripts());
    }

    private static void printCommonPlans(Set<OperatorSubPlan> common, AnalysedScript script1, AnalysedScript script2) {
        StringBuilder sb = new StringBuilder();

        sb.append("Comparing ").append(script1.getScriptName()).append(" with ").append(script2.getScriptName());
        sb.append(": ").append(common.size()).append(" match(es).");

        for (OperatorSubPlan subPlan : common) {
            sb.append("\n\t- ");
            sb.append(subPlan.size()).append(" operators");

            Operator sink = subPlan.getSinks().get(0);
            if (sink instanceof LogicalRelationalOperator) {
                sb.append(" up to `").append(((LogicalRelationalOperator) sink).getAlias()).append("`");
            }
            sb.append(" (").append(sink.getName()).append(")");
        }
        log.debug(sb.toString());
    }

    /**
     * Compares two logical plans and tries to determine common pre-processing steps. (I.e. a matching sequence of
     * operators, starting from plan sources).
     */
    public Set<OperatorSubPlan> findCommonPreprocessing() {
        return compare(plan1.getSources(), plan2.getSources());
    }

    private Set<OperatorSubPlan> compare(List<Operator> ops1, List<Operator> ops2) {
        Set<OperatorSubPlan> common = new HashSet<OperatorSubPlan>();
        if (ops1 != null && ops2 != null) {
            Iterator<Operator> iterator1 = ops1.iterator();
            Iterator<Operator> iterator2;
            while (iterator1.hasNext()) {
                Operator op1 = iterator1.next();
                iterator2 = ops2.iterator();
                while (iterator2.hasNext()) {
                    Operator op2 = iterator2.next();
                    common.addAll(compare(op1, op2));
                }
            }
        }
        return common;
    }

    private Set<OperatorSubPlan> compare(Operator op1, Operator op2) {
        if (operatorsAreEqual(op1, op2)) {
            List<Operator> successors1 = op1.getPlan().getSuccessors(op1);
            List<Operator> successors2 = op2.getPlan().getSuccessors(op2);
            Set<OperatorSubPlan> common = compare(successors1, successors2);

            // If the given operators match and we don't have any matching trees for successors,
            // then create a new subplan for this operator.
            if (common.isEmpty()) {
                OperatorSubPlan subPlan = new OperatorSubPlan(op1.getPlan());
                subPlan.add(op1);
                common.add(subPlan);
            }
            // If we have multiple matching successor plans, add the current operator to each of them.
            else {
                for (OperatorSubPlan plan : common) {
                    plan.add(op1);
                }
            }
            return common;
        }

        return new HashSet<OperatorSubPlan>();
    }

    private boolean operatorsAreEqual(Operator op1, Operator op2) {
        if (op1 == op2) {
            return true;
        }
        if (op1 == null || op2 == null) {
            return false;
        }

        boolean match = false;
        try {
            match = op1.isEqual(op2);
        } catch (Throwable e) {
            // LOSort and LOCogroup sometimes fail with exceptions (e.g. IndexOutOfBoundsException). We don't care,
            // and just assume that operators don't match in that case.
            log.debug("Failed to compare operators", e);
        }
        log.debug("Operator Comparison: " + (match ? "MATCHING" : "not matching")
            + "\n\tOp1: " + op1 + "\n\tOp2: " + op2);
        return match;
    }
}

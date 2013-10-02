package de.uni_potsdam.hpi.loddp.common;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Various utility methods for modifying an operator plan.
 */
public class OperatorPlanUtil {

    /**
     * Add the given child operator as a successor to the given parent operator.
     */
    public static void attachChild(Operator parent, Operator child) {
        parent.getPlan().add(child);
        parent.getPlan().connect(parent, child);
    }

    /**
     * Disconnects and removes the given operator.
     *
     * Assumes that the operator does not have any successors.
     */
    public static void remove(Operator operator) throws FrontendException {
        OperatorPlan plan = operator.getPlan();

        List<Operator> predecessors = plan.getPredecessors(operator);
        if (predecessors != null) {
            predecessors = new ArrayList<Operator>(predecessors); // copy to avoid concurrent modification exception
            for (Operator predecessor : predecessors) {
                plan.disconnect(predecessor, operator);
            }
        }
        plan.remove(operator);
    }

    /**
     * Retrieve all siblings of the given operator.
     */
    public static List<Operator> findSiblings(Operator operator) {
        OperatorSubPlan subPlan = findSiblings(operator, false, false);
        Iterator<Operator> siblings = subPlan.getOperators();
        List<Operator> siblingsList = new ArrayList<Operator>(subPlan.size());
        while (siblings.hasNext()) {
            siblingsList.add(siblings.next());
        }
        return siblingsList;
    }

    /**
     * Retrieve all siblings of the given operator.
     *
     * @param sameTypeOnly If set to true, only operators of the same type are returned.
     * @param matchSources Whether plan sources (i.e. operators without predecessors) are considered siblings.
     */
    public static OperatorSubPlan findSiblings(Operator operator, boolean sameTypeOnly, boolean matchSources) {
        List<Operator> parents = operator.getPlan().getPredecessors(operator);
        OperatorSubPlan siblingPlan = new OperatorSubPlan(operator.getPlan());
        if (parents != null) {
            for (Operator parent : parents) {
                List<Operator> successors = operator.getPlan().getSuccessors(parent);
                addOperators(siblingPlan, successors, operator, sameTypeOnly);
            }
        } else if (matchSources) {
            List<Operator> sources = operator.getPlan().getSources();
            addOperators(siblingPlan, sources, operator, sameTypeOnly);
        }
        return siblingPlan;
    }

    private static void addOperators(OperatorSubPlan plan, List<Operator> operators,
                                     Operator original, boolean sameTypeOnly) {
        for (Operator source : operators) {
            if (!sameTypeOnly || original.getClass() == source.getClass()) {
                plan.add(source);
            }
        }
    }

    /**
     * Replaces an operator. Assumes that both operators have the same predecessors.
     *
     * @param oldOperator
     * @param newOperator
     *
     * @throws FrontendException
     */
    public static void replace(Operator oldOperator, Operator newOperator) throws FrontendException {
        OperatorPlan plan = oldOperator.getPlan();
        plan.add(newOperator);

        List<Operator> succs = plan.getSuccessors(oldOperator);
        if (succs != null) {
            List<Operator> succsCopy = new ArrayList<Operator>();
            succsCopy.addAll(succs);
            for (int i = 0; i < succsCopy.size(); i++) {
                Operator succ = succsCopy.get(i);
                Pair<Integer, Integer> pos = plan.disconnect(oldOperator, succ);
                plan.connect(newOperator, i, succ, pos.second);
            }
        }

        remove(oldOperator);
    }
}

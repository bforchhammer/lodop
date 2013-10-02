package de.uni_potsdam.hpi.loddp.common;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;

import java.util.ArrayList;
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
        List<Operator> predecessors = new ArrayList<Operator>(plan.getPredecessors(operator)); // copy to avoid concurrent modification exception
        for (Operator predecessor : predecessors) {
            plan.disconnect(predecessor, operator);
        }
        plan.remove(operator);
    }

    /**
     * Retrieve all siblings of the given operator.
     */
    public static List<Operator> findSiblings(Operator operator) {
        List<Operator> parents = operator.getPlan().getPredecessors(operator);
        List<Operator> siblings = new ArrayList<Operator>();
        for (Operator parent : parents) {
            siblings.addAll(operator.getPlan().getSuccessors(parent));
        }
        return siblings;
    }
}

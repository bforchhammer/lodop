package de.uni_potsdam.hpi.loddp.analyser.matching;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;

import java.util.List;

/**
 * Extends OperatorSubPlan with equals() and hashCode() implementation.
 */
public class SubPlan extends OperatorSubPlan {

    protected static final Log log = LogFactory.getLog(SubPlan.class);
    private static final int HASH_PRIME_OPERATOR = 61;
    private static final int HASH_PRIME_PREDECESSOR = 67;

    public SubPlan(OperatorPlan base) {
        super(base);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof OperatorPlan)) {
            return false;
        }
        try {
            // BaseOperatorPlan.isEqual() checks that the leaves and all it's predecessors are equal.
            return super.isEqual((OperatorPlan) other);
        } catch (Throwable e) {
            log.debug("Cannot check equality for plans.", e);
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 0;

        List<Operator> leaves = getSinks();
        for (Operator op : leaves) {
            hash += hash(op);
        }

        return hash;
    }

    private int hash(Operator op) {
        int hash = 1;
        // We only hash based on the name, which means that two different plans may end up having the same
        // hash code. This is acceptable as far as the Consistency contract with equals() goes, but will most likely
        // cause hash-collisions.
        hash *= HASH_PRIME_OPERATOR + op.getName().hashCode();
        List<Operator> predecessors = this.getPredecessors(op);
        if (predecessors != null) {
            for (Operator predecessor : predecessors) {
                hash *= HASH_PRIME_PREDECESSOR + hash(predecessor);
            }
        }
        return 0;
    }
}

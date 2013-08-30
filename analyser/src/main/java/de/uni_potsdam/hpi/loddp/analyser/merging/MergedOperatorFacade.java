package de.uni_potsdam.hpi.loddp.analyser.merging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a set of operators which have been merged together because they are identical.
 *
 * Two operators are identical if they have the same predecessors in the corresponding logical plan, and match according
 * to the result of the {@link #isEqual(Operator)} method.
 */
public class MergedOperatorFacade {
    protected static final Log log = LogFactory.getLog(MergedOperatorFacade.class);
    private static final String ANNOTATION_WRAPPED_OPERATORS = "plan.merging.operators-merged";
    private Operator operator;

    /**
     * Constructor.
     *
     * @param operator An operator to wrap.
     */
    public MergedOperatorFacade(Operator operator) {
        this.operator = operator;
    }

    public static MergedOperatorFacade decorate(Operator operator) {
        return new MergedOperatorFacade(operator);
    }

    public List<Operator> getMergedOperators() {
        ArrayList<Operator> operators = (ArrayList<Operator>) operator.getAnnotation(ANNOTATION_WRAPPED_OPERATORS);
        return operators;
    }

    public void addMergedOperator(Operator op) {
        List<Operator> operators = getMergedOperators();
        if (operators == null) {
            operators = new ArrayList<Operator>();
            operator.annotate(ANNOTATION_WRAPPED_OPERATORS, operators);
        }
        if (!operators.contains(op)) {
            operators.add(op);
        }
    }

    /**
     * Return true if the given operator matches the decorated operator.
     *
     * @param operator An operator to compare against.
     *
     * @return TRUE if operators match, FALSE otherwise.
     *
     * @throws FrontendException
     */
    public boolean isEqual(Operator operator) throws FrontendException {
        if (operator == null) {
            return false;
        }
        if (operator == this.operator) {
            return true;
        }
        // Check whether the given operator is the same as any of the merged operator objects.
        List<Operator> merged = getMergedOperators();
        if (merged != null && merged.contains(operator)) {
            return true;
        }
        return this.operator.isEqual(operator);
    }

    /**
     * Check if the given operator matches the decorated one. In case of success, add the operator to the list of
     * wrapped operators and return TRUE. Otherwise return FALSE.
     *
     * @param operator
     *
     * @return TRUE if given operator can be merged in (or has already been merged in); FALSE otherwise.
     */
    public boolean merge(Operator operator) {
        try {
            if (isEqual(operator)) {
                addMergedOperator(operator);
                return true;
            }
        } catch (Throwable e) {
            log.debug("Failed to compare operators.", e);
        }
        return false;
    }
}

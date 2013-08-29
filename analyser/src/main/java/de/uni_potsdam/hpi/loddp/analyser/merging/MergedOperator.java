package de.uni_potsdam.hpi.loddp.analyser.merging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.parser.SourceLocation;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a set of operators which have been merged together because they are identical.
 *
 * Two operators are identical if they have the same predecessors in the corresponding logical plan, and match according
 * to the result of the {@link #isEqual(Operator)} method.
 */
public class MergedOperator extends Operator {
    protected static final Log log = LogFactory.getLog(MergedOperator.class);
    private List<Operator> wrapped;

    /**
     * Constructor.
     *
     * @param operator   An operator to wrap.
     * @param mergedPlan The merged plan which this merged operator belongs to.
     */
    public MergedOperator(Operator operator, OperatorPlan mergedPlan) {
        super(operator.getName(), mergedPlan);
        wrapped = new ArrayList<Operator>();
        wrapped.add(operator);
    }

    /**
     * Returns the name of the operator.
     *
     * This matches the name of the first wrapped operator. For logical operators, the alias (wrapped in round brackets)
     * is appended to the name.
     */
    @Override
    public String getName() {
        Operator op = wrapped.get(0);
        if (op instanceof LogicalRelationalOperator) {
            StringBuffer sb = new StringBuffer();
            sb.append(op.getName());
            sb.append(" (");
            sb.append(((LogicalRelationalOperator) op).getAlias());
            sb.append(")");
            return sb.toString();
        }
        return super.getName();
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        throw new FrontendException("Not implemented");
        /*// @todo this may not work as expected (?)
        for (Operator op : wrapped) {
            op.accept(v);
        }*/
    }

    /**
     * Return true if the given operator matches this merged operator.
     *
     * Equality is checked by comparing against the first wrapped operator. If the given operator also is a merged
     * operator only first wrapped operators are compared.
     *
     * @param operator An operator to compare against.
     *
     * @return TRUE if operators match, FALSE otherwise.
     *
     * @throws FrontendException
     */
    @Override
    public boolean isEqual(Operator operator) throws FrontendException {
        if (operator == null) {
            return false;
        }
        if (operator == this) {
            return true;
        }

        // If we're comparing against another merged operator, compare their first wrapped operators instead.
        if (operator instanceof MergedOperator) {
            operator = ((MergedOperator) operator).wrapped.get(0);
        }

        // Check whether the given operator is the same as any of the contained wrapped operator objects.
        for (Operator op : wrapped) {
            if (operator == op) return true;
        }

        return wrapped.get(0).isEqual(operator);
    }

    @Override
    public SourceLocation getLocation() {
        throw new UnsupportedOperationException("MergedOperator does not implemented getLocation().");
    }

    @Override
    public void setLocation(SourceLocation loc) {
        throw new UnsupportedOperationException("MergedOperator does not implemented setLocation(SourceLocation).");
    }

    /**
     * Set an annotation value on all wrapped operators.
     *
     * @param key
     * @param val
     */
    public void annotateWrapped(String key, Object val) {
        for (Operator op : wrapped) {
            op.annotate(key, val);
        }
    }

    /**
     * Remove an annotation value from all wrapped operators.
     *
     * @param key
     */
    public void removeWrappedAnnotations(String key) {
        for (Operator op : wrapped) {
            op.removeAnnotation(key);
        }
    }

    /**
     * Get an annotation value from the first wrapped operator.
     *
     * @param key
     *
     * @return
     */
    public Object getWrappedAnnotation(String key) {
        return getWrappedAnnotation(0, key);
    }

    /**
     * Get an annotation value from a wrapped operator.
     *
     * @param index
     * @param key
     *
     * @return
     */
    public Object getWrappedAnnotation(int index, String key) {
        return wrapped.get(index).getAnnotation(key);
    }

    /**
     * Remove an annotation value from a wrapped operator.
     *
     * @param index
     * @param key
     *
     * @return
     */
    public Object removeWrappedAnnotation(int index, String key) {
        return wrapped.get(index).removeAnnotation(key);
    }

    /**
     * @return The number of wrapped operators.
     */
    public int size() {
        return wrapped.size();
    }

    /**
     * Check if the given operator matches the already wrapped one. In case of success, add the operator to the list of
     * wrapped operators and return TRUE. Otherwise return FALSE.
     *
     * @param operator
     *
     * @return TRUE if given operator can be merged in (or has already been merged in); FALSE otherwise.
     */
    public boolean merge(Operator operator) {
        try {
            if (isEqual(operator)) {
                if (!wrapped.contains(operator)) {
                    wrapped.add(operator);
                }
                return true;
            }
        } catch (Throwable e) {
            log.debug("Failed to compare operators.", e);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getName());
        for (int i = 0; i < wrapped.size(); i++) {
            sb.append("\n\t\t ==> ");
            sb.append(wrapped.get(i).toString());
        }
        return sb.toString();
    }
}

package de.uni_potsdam.hpi.loddp.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for counting operators in plans. Works with both Pig's old and new plan architectures.
 */
public class OperatorCounter {
    private static final Log log = LogFactory.getLog(OperatorCounter.class);
    private Map<String, AtomicInteger> counters = new HashMap<String, AtomicInteger>();
    private String name;

    public OperatorCounter(String name) {
        this.name = name;
    }

    public void count(LogicalPlan plan) {
        try {
            new LOVisitor(plan).visit();
        } catch (FrontendException e) {
            log.error("Failed to count operators.", e);
        }
    }

    public void count(PhysicalPlan plan) {
        try {
            new OldPlanVisitor<PhysicalOperator, PhysicalPlan>(plan).visit();
        } catch (FrontendException e) {
            log.error("Failed to count operators.", e);
        }
    }

    public void count(MROperPlan plan) {
        try {
            new OldPlanVisitor<MapReduceOper, MROperPlan>(plan).visit();
        } catch (FrontendException e) {
            log.error("Failed to count operators.", e);
        }
    }

    public void dump() {
        StringBuffer output = new StringBuffer("Number of operators: " + name);
        for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
            output.append("\n").append(entry.getKey()).append("\t").append(entry.getValue());
        }
        log.info(output);
    }

    protected void increment(String type) {
        if (counters.containsKey(type)) {
            counters.get(type).incrementAndGet();
        } else {
            counters.put(type, new AtomicInteger(1));
        }
    }

    protected class LOVisitor extends PlanVisitor {
        public LOVisitor(OperatorPlan plan) throws FrontendException {
            super(plan, new Walker(plan));
        }

        public void visit(LogicalRelationalOperator operator) throws FrontendException {
            increment(operator.getName());
        }
    }

    protected class Walker extends DependencyOrderWalker {
        public Walker(OperatorPlan plan) {
            super(plan);
        }

        @Override
        public void walk(PlanVisitor visitor) throws FrontendException {
            // Straight copy from super.walk()
            List<Operator> fifo = new ArrayList<Operator>();
            Set<Operator> seen = new HashSet<Operator>();
            List<Operator> leaves = plan.getSinks();
            if (leaves == null) return;
            for (Operator op : leaves) {
                doAllPredecessors(op, seen, fifo);
            }

            for (Operator op : fifo) {
                // Work-around
                if (op instanceof LogicalRelationalOperator && visitor instanceof LOVisitor) {
                    ((LOVisitor) visitor).visit((LogicalRelationalOperator) op);
                }
            }
        }
    }

    protected class OldPlanVisitor<O extends org.apache.pig.impl.plan.Operator, P extends org.apache.pig.impl.plan
        .OperatorPlan<O>> extends org.apache.pig.impl.plan.PlanVisitor<O, P> {
        public OldPlanVisitor(P plan) throws FrontendException {
            super(plan, new OldPlanWalker<O, P>(plan));
        }

        public void visit(O operator) {
            increment(operator.getClass().getSimpleName());
        }
    }

    protected class OldPlanWalker<O extends org.apache.pig.impl.plan.Operator,
        P extends org.apache.pig.impl.plan.OperatorPlan<O>>
        extends org.apache.pig.impl.plan.DependencyOrderWalker<O, P> {

        public OldPlanWalker(P plan) {
            super(plan);
        }

        @Override
        public void walk(org.apache.pig.impl.plan.PlanVisitor<O, P> visitor) throws VisitorException {
            // Straight copy from super.walk()
            List<O> fifo = new ArrayList<O>();
            Set<O> seen = new HashSet<O>();
            List<O> leaves = mPlan.getLeaves();
            if (leaves == null) return;
            for (O op : leaves) {
                doAllPredecessors(op, seen, fifo);
            }
            for (O op : fifo) {
                // Work-around
                if (visitor instanceof OldPlanVisitor) {
                    ((OldPlanVisitor) visitor).visit(op);
                }
            }
        }
    }

}

package de.uni_potsdam.hpi.loddp.common.printing;

import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanDumper;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.*;

import java.io.PrintStream;
import java.util.*;

/**
 * Print logical plans as DOT graph graphs.
 *
 * Large portions of this class are more or less identical to code from {@link org.apache.pig.newplan.DotPlanDumper} and
 * {@link org.apache.pig.newplan.logical.DotLOPrinter}, which unfortunately are not very extensible.
 *
 * Different to {@link org.apache.pig.newplan.logical.DotLOPrinter}, this class also prints soft links (dashed).
 */
public class LogicalPlanPrinter extends PlanDumper {
    protected Set<Operator> mSubgraphs;
    protected Set<Operator> mMultiInputSubgraphs;
    protected Set<Operator> mMultiOutputSubgraphs;
    protected boolean isSubGraph = false;

    public LogicalPlanPrinter(BaseOperatorPlan plan, PrintStream ps) {
        this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
            new HashSet<Operator>());
    }

    protected LogicalPlanPrinter(BaseOperatorPlan plan, PrintStream ps, boolean isSubGraph, Set<Operator> mSubgraphs,
                                 Set<Operator> mMultiOutputSubgraphs, Set<Operator> mMultiInputSubgraphs) {
        super(plan, ps);
        this.mSubgraphs = mSubgraphs;
        this.isSubGraph = isSubGraph;
        this.mMultiOutputSubgraphs = mMultiOutputSubgraphs;
        this.mMultiInputSubgraphs = mMultiInputSubgraphs;
    }

    @Override
    protected PlanDumper makeDumper(BaseOperatorPlan plan, PrintStream ps) {
        return new LogicalPlanPrinter(plan, ps, true, mSubgraphs, mMultiInputSubgraphs, mMultiOutputSubgraphs);
    }

    @Override
    public void dump() {
        if (!isSubGraph) {
            ps.println("digraph plan {");
            ps.println("compound=true;");
            //ps.println("ratio=auto;");
            ps.println("rankdir=LR;");
            ps.println("forcelabels=true;");
            ps.println("node [shape=rect];");
        }
        super.dump();
        dumpSoftLinks();
        if (!isSubGraph) {
            ps.println("}");
        }
    }

    @Override
    protected MultiMap<Operator, BaseOperatorPlan> getMultiInputNestedPlans(Operator op) {

        if (op instanceof LOCogroup) {
            MultiMap<Operator, BaseOperatorPlan> planMap = new MultiMap<Operator, BaseOperatorPlan>();
            for (Integer i : ((LOCogroup) op).getExpressionPlans().keySet()) {
                List<BaseOperatorPlan> plans = new ArrayList<BaseOperatorPlan>();
                plans.addAll(((LOCogroup) op).getExpressionPlans().get(i));
                Operator pred = plan.getPredecessors(op).get(i);
                planMap.put(pred, plans);
                //pred = plan.getSoftLinkPredecessors(op).get(i);
                //planMap.put(pred, plans);
            }
            return planMap;
        } else if (op instanceof LOJoin) {
            MultiMap<Operator, BaseOperatorPlan> planMap = new MultiMap<Operator, BaseOperatorPlan>();
            for (Integer i : ((LOJoin) op).getExpressionPlans().keySet()) {
                List<BaseOperatorPlan> plans = new ArrayList<BaseOperatorPlan>();
                plans.addAll(((LOJoin) op).getExpressionPlans().get(i));
                Operator pred = plan.getPredecessors(op).get(i);
                planMap.put(pred, plans);
                //pred = plan.getSoftLinkPredecessors(op).get(i);
                //planMap.put(pred, plans);
            }
            return planMap;
        }
        return new MultiMap<Operator, BaseOperatorPlan>();
    }

    @Override
    protected Collection<BaseOperatorPlan> getNestedPlans(Operator op) {
        Collection<BaseOperatorPlan> plans = new LinkedList<BaseOperatorPlan>();

        if (op instanceof LOFilter) {
            plans.add(((LOFilter) op).getFilterPlan());
        } else if (op instanceof LOLimit) {
            plans.add(((LOLimit) op).getLimitPlan());
        } else if (op instanceof LOForEach) {
            plans.add(((LOForEach) op).getInnerPlan());
        } else if (op instanceof LOGenerate) {
            plans.addAll(((LOGenerate) op).getOutputPlans());
        } else if (op instanceof LOSort) {
            plans.addAll(((LOSort) op).getSortColPlans());
        } else if (op instanceof LOSplitOutput) {
            plans.add(((LOSplitOutput) op).getFilterPlan());
        }

        return plans;
    }

    protected boolean reverse(BaseOperatorPlan plan) {
        if (plan instanceof LogicalExpressionPlan)
            return true;
        return false;
    }

    @Override
    protected void dumpMultiInputNestedOperator(Operator op, MultiMap<Operator, BaseOperatorPlan> plans) {
        dumpInvisibleOutput(op);

        ps.print("subgraph ");
        ps.print(getClusterID(op));
        ps.println(" {");
        ps.print(attributesToString(getAttributes(op), "; "));
        ps.println("; labelloc=b;");

        mMultiInputSubgraphs.add(op);

        for (Operator o : plans.keySet()) {
            ps.print("subgraph ");
            ps.print(getClusterID(op, o));
            ps.println(" {");
            ps.println("label=\"\";");
            dumpInvisibleInput(op, o);
            for (BaseOperatorPlan plan : plans.get(o)) {
                PlanDumper dumper = makeDumper(plan, ps);
                dumper.dump();
                connectInvisibleInput(op, o, plan);
            }
            ps.println("};");
        }
        ps.println("};");

        for (Operator o : plans.keySet()) {
            for (BaseOperatorPlan plan : plans.get(o)) {
                connectInvisibleOutput(op, plan);
            }
        }
    }

    @Override
    protected void dumpMultiOutputNestedOperator(Operator op, Collection<BaseOperatorPlan> plans) {
        super.dumpMultiOutputNestedOperator(op, plans);

        mMultiOutputSubgraphs.add(op);

        dumpInvisibleOutput(op);
        for (BaseOperatorPlan plan : plans) {
            connectInvisibleOutput(op, plan);
        }
    }

    @Override
    protected void dumpNestedOperator(Operator op, Collection<BaseOperatorPlan> plans) {
        dumpInvisibleOperators(op);
        ps.print("subgraph ");
        ps.print(getClusterID(op));
        ps.println(" {");
        ps.print(attributesToString(getAttributes(op), "; "));
        ps.println("; labelloc=b;");

        mSubgraphs.add(op);

        for (BaseOperatorPlan plan : plans) {
            PlanDumper dumper = makeDumper(plan, ps);
            dumper.dump();
            connectInvisibleInput(op, plan);
        }
        ps.println("};");

        for (BaseOperatorPlan plan : plans) {
            connectInvisibleOutput(op, plan);
        }
    }

    @Override
    protected void dumpOperator(Operator op) {
        ps.print(getID(op));
        ps.print(" [");
        ps.print(attributesToString(getAttributes(op), ", "));
        ps.println("];");
    }

    @Override
    protected void dumpEdge(Operator op, Operator suc) {
        String in = getID(op);
        String out = getID(suc);
        Map<String, String> attributes = getEdgeAttributes(op, suc);

        if (mMultiInputSubgraphs.contains(op)
            || mSubgraphs.contains(op)
            || mMultiOutputSubgraphs.contains(op)) {
            in = getSubgraphID(op, false);
        }

        if (mMultiInputSubgraphs.contains(suc)) {
            out = getSubgraphID(suc, op, true);
            attributes.put("lhead", getClusterID(suc, op));
        }

        if (mSubgraphs.contains(suc)) {
            out = getSubgraphID(suc, true);
            attributes.put("lhead", getClusterID(suc));
        }

        if (reverse(plan)) {
            ps.print(out);
            ps.print(" -> ");
            ps.print(in);
        } else {
            ps.print(in);
            ps.print(" -> ");
            ps.print(out);
        }
        ps.println(" [" + attributesToString(attributes) + "]");
    }

    private void connectInvisibleInput(Operator op1, Operator op2, BaseOperatorPlan plan) {
        String in = getSubgraphID(op1, op2, true);

        List<Operator> sources;
        if (reverse(plan))
            sources = plan.getSinks();
        else
            sources = plan.getSources();

        for (Operator l : sources) {
            dumpInvisibleEdge(in, getID(l));
        }
    }

    private void connectInvisibleInput(Operator op, BaseOperatorPlan plan) {
        String in = getSubgraphID(op, true);

        List<Operator> sources;
        if (reverse(plan))
            sources = plan.getSinks();
        else
            sources = plan.getSources();

        for (Operator l : sources) {
            String out;
            if (mSubgraphs.contains(l) || mMultiInputSubgraphs.contains(l)) {
                out = getSubgraphID(l, true);
            } else {
                out = getID(l);
            }

            dumpInvisibleEdge(in, out);
        }
    }

    private void connectInvisibleOutput(Operator op, BaseOperatorPlan plan) {
        String out = getSubgraphID(op, false);

        List<Operator> sinks;
        if (reverse(plan))
            sinks = plan.getSources();
        else
            sinks = plan.getSinks();

        for (Operator l : sinks) {
            String in;
            if (mSubgraphs.contains(l)
                || mMultiInputSubgraphs.contains(l)
                || mMultiOutputSubgraphs.contains(l)) {
                in = getSubgraphID(l, false);
            } else {
                in = getID(l);
            }

            dumpInvisibleEdge(in, out);
        }
    }

    private void connectInvisible(Operator op, BaseOperatorPlan plan) {
        connectInvisibleInput(op, plan);
        connectInvisibleOutput(op, plan);
    }

    private void dumpInvisibleInput(Operator op1, Operator op2) {
        ps.print(getSubgraphID(op1, op2, true));
        ps.print(" ");
        ps.print(getInvisibleAttributes(op1));
        ps.println(";");
    }

    private void dumpInvisibleInput(Operator op) {
        ps.print(getSubgraphID(op, true));
        ps.print(" ");
        ps.print(getInvisibleAttributes(op));
        ps.println(";");
    }

    private void dumpInvisibleOutput(Operator op) {
        ps.print(getSubgraphID(op, false));
        ps.print(" ");
        ps.print(getInvisibleAttributes(op));
        ps.println(";");
    }

    protected void dumpInvisibleOperators(Operator op) {
        dumpInvisibleInput(op);
        dumpInvisibleOutput(op);
    }

    private String getClusterID(Operator op1, Operator op2) {
        return getClusterID(op1) + "_" + getID(op2);
    }

    private String getClusterID(Operator op) {
        return "cluster_" + getID(op);
    }

    private String getSubgraphID(Operator op1, Operator op2, boolean in) {
        String id = "s" + getID(op1) + "_" + getID(op2);
        if (in) {
            id += "_in";
        } else {
            id += "_out";
        }
        return id;
    }

    private String getSubgraphID(Operator op, boolean in) {
        String id = "s" + getID(op);
        if (in) {
            id += "_in";
        } else {
            id += "_out";
        }
        return id;
    }

    private String getID(Operator op) {
        return "" + Math.abs(op.hashCode());
    }

    private String getInvisibleAttributes(Operator op) {
        return "[shape=point, margin=0, label=\"\", style=invis, height=0, width=0]";
    }

    private void dumpInvisibleEdge(String op, String suc) {
        ps.print(op);
        ps.print(" -> ");
        ps.print(suc);
        ps.println(" [style=invis];");
    }

    protected void dumpSoftLinks() {
        Iterator<Operator> iterator = plan.getOperators();
        while (iterator.hasNext()) {
            Operator op = iterator.next();
            Collection<Operator> successors = plan.getSoftLinkSuccessors(op);
            if (successors != null) {
                for (Operator suc : successors) {
                    dumpSoftLink(op, suc);
                }
            }
        }
    }

    protected void dumpSoftLink(Operator op, Operator suc) {
        String in = getID(op);
        String out = getID(suc);
        Map<String, String> attributes = getSoftLinkAttributes(op, suc);

        if (mMultiInputSubgraphs.contains(op)
            || mSubgraphs.contains(op)
            || mMultiOutputSubgraphs.contains(op)) {
            in = getSubgraphID(op, false);
        }

        if (mMultiInputSubgraphs.contains(suc)) {
            out = getSubgraphID(suc, op, true);
            attributes.put("lhead", getClusterID(suc, op));
        }

        if (mSubgraphs.contains(suc)) {
            out = getSubgraphID(suc, true);
            attributes.put("lhead", getClusterID(suc));
        }

        if (reverse(plan)) {
            ps.print(out);
            ps.print(" -> ");
            ps.print(in);
        } else {
            ps.print(in);
            ps.print(" -> ");
            ps.print(out);
        }
        ps.println(" [" + attributesToString(attributes) + "]");
    }

    /**
     * Used to generate the label for an operator.
     *
     * @param op operator to dump
     */
    protected String getName(Operator op) {
        StringBuffer sb = new StringBuffer(op.getName());
        if (op instanceof ProjectExpression) {
            ProjectExpression pr = (ProjectExpression) op;
            sb.append(pr.getInputNum());
            sb.append(":");
            if (pr.isProjectStar())
                sb.append("(*)");
            else if (pr.isRangeProject())
                sb.append("[").append(pr.getStartCol()).append(" .. ").append(pr.getEndCol()).append("]");
            else
                sb.append(pr.getColNum());
        } else if (op instanceof LOInnerLoad) {
            sb.append("\\n").append(getName(((LOInnerLoad) op).getProjection()));
        }
        if (op instanceof LogicalRelationalOperator) {
            LogicalRelationalOperator operator = (LogicalRelationalOperator) op;
            if (operator.getAlias() != null) {
                sb.append("\\n(").append(operator.getAlias()).append(")");
            }
        }
        return sb.toString();
    }

    /**
     * Generate attributes for a node.
     */
    protected Map<String, String> getAttributes(Operator op) {
        Map<String, String> attributes = new HashMap<String, String>();

        attributes.put("style", "filled");
        attributes.put("fillcolor", "#F5F5F5");

        String label = getName(op);
        if (op instanceof LOStore || op instanceof LOLoad) {
            label.replace(":", ",\\n");
            attributes.put("fillcolor", "gray");
        }
        attributes.put("label", label);

        return attributes;
    }

    /**
     * Generate attributes for an edge.
     */
    protected Map<String, String> getEdgeAttributes(Operator pred, Operator suc) {
        Map<String, String> attributes = new HashMap<String, String>();
        return attributes;
    }

    /**
     * Generate attributes for a soft link.
     */
    protected Map<String, String> getSoftLinkAttributes(Operator pred, Operator suc) {
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("style", "dashed");
        return attributes;
    }

    /**
     * Convert an attributes map to a comma-separated string.
     */
    protected String attributesToString(Map<String, String> attributes) {
        return attributesToString(attributes, ", ");
    }

    /**
     * Convert an attributes map to a comma-separated string.
     */
    protected String attributesToString(Map<String, String> attributes, String separator) {
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            if (sb.length() > 0) sb.append(separator);
            sb.append(entry.getKey()).append("=").append(quote(entry.getValue()));
        }
        return sb.toString();
    }

    /**
     * Adds quotes around the given string value. Also makes sure that the contained string does not contain quotes
     * itself.
     */
    protected String quote(String value) {
        return "\"" + value.replace('"', '\'') + "\"";
    }
}

package de.uni_potsdam.hpi.loddp.common.printing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanDumper;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.logical.relational.LOFilter;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;

public class LOFilterPrinter extends LogicalPlanPrinter {
    protected static final Log log = LogFactory.getLog(LogicalPlanPrinter.class);

    public LOFilterPrinter(BaseOperatorPlan plan, PrintStream ps) {
        super(plan, ps);
    }

    public LOFilterPrinter(BaseOperatorPlan plan, PrintStream ps, boolean isSubGraph, Set<Operator> mSubgraphs, Set<Operator> mMultiOutputSubgraphs, Set<Operator> mMultiInputSubgraphs) {
        super(plan, ps, isSubGraph, mSubgraphs, mMultiOutputSubgraphs, mMultiInputSubgraphs);
    }

    @Override
    protected PlanDumper makeDumper(BaseOperatorPlan plan, PrintStream ps) {
        return new LOFilterPrinter(plan, ps, true, mSubgraphs, mMultiInputSubgraphs, mMultiOutputSubgraphs);
    }

    /**
     * Used to generate the label for an operator.
     *
     * @param op operator to dump
     */
    @Override
    protected String getName(Operator op) {
        if (op instanceof LOFilter) {
            StringBuilder sb = new StringBuilder(op.getName());
            filterConditionToString(sb, ((LOFilter) op).getFilterPlan());
            return sb.toString();
        }
        return super.getName(op);
    }

    protected String getExpressionOperatorName(LogicalExpression expression) {
        if (expression instanceof AddExpression) return "+";
        if (expression instanceof AndExpression) return "&&";
        if (expression instanceof CastExpression) return "";
        if (expression instanceof ConstantExpression) return ((ConstantExpression) expression).getValue().toString();
        if (expression instanceof DereferenceExpression) {
            List<Object> rawColumns = ((DereferenceExpression) expression).getRawColumns();
            if (rawColumns != null && !rawColumns.isEmpty()) {
                if (rawColumns.size() == 1) {
                    return "[" + rawColumns.get(0) + "]";
                } else {
                    return "[ERR]";
                }
            } else {
                List<Integer> columns = ((DereferenceExpression) expression).getBagColumns();
                if (columns.size() == 1) {
                    return "[" + columns.get(0) + "]";
                } else {
                    return "[ERR]";
                }
            }
        }
        if (expression instanceof DivideExpression) return "/";
        if (expression instanceof EqualExpression) return "==";
        if (expression instanceof GreaterThanEqualExpression) return ">=";
        if (expression instanceof GreaterThanExpression) return ">";
        if (expression instanceof IsNullExpression) return "is NULL";
        if (expression instanceof LessThanEqualExpression) return "<=";
        if (expression instanceof LessThanExpression) return "<";
        if (expression instanceof MapLookupExpression) return "." + ((MapLookupExpression) expression).getLookupKey();
        //if (expression instanceof ModExpression) return "%";
        if (expression instanceof MultiplyExpression) return "*";
        if (expression instanceof NegativeExpression) return "-";
        if (expression instanceof NotEqualExpression) return "!=";
        if (expression instanceof NotExpression) return "!";
        if (expression instanceof OrExpression) return "||";
        if (expression instanceof ProjectExpression) {
            ProjectExpression pr = (ProjectExpression) expression;
            if (pr.isProjectStar())
                return "$*";
            else if (pr.isRangeProject())
                return "$" + pr.getStartCol() + ".." + pr.getEndCol();
            else
                return "$" + pr.getColNum();
        }
        //if (expression instanceof RegexExpression) return "";
        //if (expression instanceof ScalarExpression) return "";
        if (expression instanceof SubtractExpression) return "-";
        if (expression instanceof UserFuncExpression) return ((UserFuncExpression) expression).getFuncSpec()
            .getClassName();

        return expression.getName();
    }

    protected void filterConditionToString(StringBuilder sb, LogicalExpressionPlan expressionPlan) {
        List<Operator> sources = expressionPlan.getSources();
        for (Operator source : sources) {
            sb.append("\\n");
            try {
                filterConditionToString(sb, source);
            } catch (FrontendException e) {
                sb.append("[print-error]");
                log.error("Failed to stringify filter condition.", e);
            }
        }
    }

    protected void filterConditionToString(StringBuilder sb, Operator operator) throws FrontendException {
        if (operator instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) operator;
            sb.append("( ");
            filterConditionToString(sb, binaryExpression.getLhs());
            sb.append(" ").append(getExpressionOperatorName(binaryExpression)).append(" ");
            filterConditionToString(sb, binaryExpression.getRhs());
            sb.append(" )");
        } else if (operator instanceof UnaryExpression) {
            UnaryExpression unaryExpression = (UnaryExpression) operator;
            // Operation displayed after argument.
            if (operator instanceof IsNullExpression) {
                filterConditionToString(sb, unaryExpression.getExpression());
                sb.append(getExpressionOperatorName(unaryExpression));
            }
            // Default: operation displayed before argument.
            else {
                sb.append(getExpressionOperatorName(unaryExpression));
                filterConditionToString(sb, unaryExpression.getExpression());
            }
        } else if (operator instanceof ColumnExpression) {
            List<Operator> inputs = operator.getPlan().getSuccessors(operator);
            if (inputs != null) {
                if (inputs.size() == 1)
                    filterConditionToString(sb, inputs.get(0));
                else
                    sb.append("[print-error]");
            }
            sb.append(getExpressionOperatorName((ColumnExpression) operator));
        } else if (operator instanceof UserFuncExpression) {
            UserFuncExpression userFuncExpression = (UserFuncExpression) operator;
            sb.append(getExpressionOperatorName(userFuncExpression));
            List<LogicalExpression> arguments = userFuncExpression.getArguments();
            sb.append("(");
            boolean first = true;
            for (LogicalExpression argument : arguments) {
                if (!first) {
                    sb.append(", ");
                    first = false;
                }
                filterConditionToString(sb, argument);
            }
            sb.append(")");
        }
    }


}


package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.PigContextUtil;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.*;
import org.apache.pig.parser.QueryParserDriver;
import org.apache.pig.parser.QueryParserUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ScriptAnalyser {
    protected static final Log log = LogFactory.getLog(ScriptAnalyser.class);
    private static final String loadStatement = "quads = LOAD 'file.nq.gz' USING de.wbsg.loddesc" +
        ".importer.QuadLoader() AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray," +
        "dtlang:chararray), graph:chararray);";
    private static final AtomicInteger scopeCounter = new AtomicInteger(0);
    private final PigContext pigContext;

    public ScriptAnalyser() throws IOException {
        this(createPigContext());
    }

    public ScriptAnalyser(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    protected static PigContext createPigContext() throws IOException {
        PigContext pigContext = PigContextUtil.getContext();

        // Used to skip some validation rules, e.g. checking of input/output files.
        pigContext.inExplain = true;

        // Initialise connection to local Hadoop instance.
        pigContext.connect();

        return pigContext;
    }

    public AnalysedScript analyse(PigScript script) throws IOException {
        // Reset UDF context (done because PigServer does it).
        UDFContext.getUDFContext().reset();
        UDFContext.getUDFContext().setClientSystemProps(pigContext.getProperties());

        Map<String, String> fileNameMap = new HashMap<String, String>();
        Map<String, Operator> operators;
        String scope = "" + scopeCounter.incrementAndGet();

        // Construct query with the QuadLoader LOAD operator.
        String query = loadStatement + '\n' + script.getContent();

        // Parse the query, which gives us a list of operators and an unoptimized logical plan.
        QueryParserDriver parserDriver = new QueryParserDriver(pigContext, scope, fileNameMap);
        LogicalPlan logicalPlan = parserDriver.parse(query);
        operators = parserDriver.getOperators();

        // Add a fake STORE operation for the last alias in the script.
        QueryParserUtils.attachStorePlan(scope, logicalPlan, "fakefile", null, operators.get(pigContext.getLastAlias()), "fake", pigContext);

        // Perform some initial logical plan optimizations (copied straight from PigServer).
        optimize(logicalPlan, pigContext, scope);

        // Now, if our target alias wasn't the "last one" in the current plan, we would probably have to build a new
        // plan before we continue, see {@link PigServer.Graph#buildPlan}

        // Build physical plan. Note: this also applies optimizations to the logical plan! The un-optimized plan
        // is still available from pigContext.getExecutionEngine().getNewPlan() if needed.
        PhysicalPlan physicalPlan = pigContext.getExecutionEngine().compile(logicalPlan, null);
        LogicalPlan unoptimizedLogicalPlan = pigContext.getExecutionEngine().getNewPlan();

        // Build map-reduce plan.
        MapRedUtil.checkLeafIsStore(physicalPlan, pigContext);
        MapReduceLauncher launcher = new MapReduceLauncher();
        MROperPlan mapReducePlan = launcher.compile(physicalPlan, pigContext);

        return new AnalysedScript(script, logicalPlan, unoptimizedLogicalPlan, physicalPlan, mapReducePlan);
    }

    /**
     * Duplicates code from {@link org.apache.pig.PigServer.Graph#compile(LogicalPlan)}.
     *
     * @throws org.apache.pig.impl.logicalLayer.FrontendException
     *
     */
    private void optimize(LogicalPlan lp, PigContext pigContext, String scope) throws FrontendException {
        new ColumnAliasConversionVisitor(lp).visit();
        new SchemaAliasVisitor(lp).visit();
        new ScalarVisitor(lp, pigContext, scope).visit();

        CompilationMessageCollector collector = new CompilationMessageCollector();

        new TypeCheckingRelVisitor(lp, collector).visit();
        for (Enum type : CompilationMessageCollector.MessageType.values()) {
            CompilationMessageCollector.logAllMessages(collector, log);
        }

        new UnionOnSchemaSetter(lp).visit();
        new CastLineageSetter(lp, collector).visit();
        new ScalarVariableValidator(lp).visit();
    }
}

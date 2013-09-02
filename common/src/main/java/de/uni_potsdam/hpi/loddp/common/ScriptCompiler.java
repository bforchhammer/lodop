package de.uni_potsdam.hpi.loddp.common;

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

/**
 * Transforms pig scripts into logical, physical, and map reduce plans.
 */
public class ScriptCompiler {

    protected static final Log log = LogFactory.getLog(ScriptCompiler.class);
    private static final AtomicInteger scopeCounter = new AtomicInteger(0);
    private String currentScope;
    private PigContext pigContext;
    private String pigQuery;
    private String outputFilename;
    private LogicalPlan logicalPlan;
    private LogicalPlan optimizedLogicalPlan;
    private PhysicalPlan physicalPlan;
    private MROperPlan mapReducePlan;
    private Map<String, Operator> logicalPlanOperators;

    public ScriptCompiler(PigContext pigContext, PigScript pigScript, String inputFilename, String outputFilename) {
        this.pigContext = pigContext;
        this.pigQuery = buildLoadStatement(inputFilename);
        this.pigQuery += pigScript.getContent();
        this.outputFilename = outputFilename;
    }

    public ScriptCompiler(PigContext pigContext, LogicalPlan logicalPlan, boolean optimized) {
        this.pigContext = pigContext;
        if (optimized) {
            this.optimizedLogicalPlan = logicalPlan;
        } else {
            this.logicalPlan = logicalPlan;
        }
    }

    private String buildLoadStatement(String filename) {
        /*if (!getPig().existsFile(filename)) {
            throw new IOException("File not found on HDFS: " + filename);
        }*/
        String statement = "quads = LOAD '" + filename + "' USING de.wbsg.loddesc.importer.QuadLoader() AS " +
            "(subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), " +
            "graph:chararray); ";
        return statement;
    }

    public LogicalPlan getLogicalPlan() throws ScriptCompilationException {
        if (logicalPlan == null) {
            if (pigQuery == null || pigQuery.isEmpty()) {
                throw new ScriptCompilationException("No pig query found, cannot compile logical plan.");
            }

            try {
                // Create scope for current plan.
                currentScope = "" + scopeCounter.incrementAndGet();

                // Compile the logical plan.
                compileLogicalPlan(pigQuery);

                // Attach a STORE operator for the last alias in the script.
                QueryParserUtils.attachStorePlan(currentScope, logicalPlan, outputFilename, null,
                    logicalPlanOperators.get(pigContext.getLastAlias()), "store", pigContext);

                // Perform some initial logical plan optimizations (copied straight from PigServer).
                optimizeLogicalPlan();

            } catch (IOException e) {
                throw new ScriptCompilationException("Failed to compile logical plan", e);
            }
        }
        return logicalPlan;
    }

    private void compileLogicalPlan(String query) throws IOException {
        // Reset UDF context (done because PigServer does it).
        UDFContext.getUDFContext().reset();
        UDFContext.getUDFContext().setClientSystemProps(pigContext.getProperties());

        Map<String, String> fileNameMap = new HashMap<String, String>();

        // Parse the query, which gives us a list of operators and an unoptimized logical plan.
        QueryParserDriver parserDriver = new QueryParserDriver(pigContext, currentScope, fileNameMap);
        logicalPlan = parserDriver.parse(query);
        logicalPlanOperators = parserDriver.getOperators();
    }

    /**
     * Duplicates code from {@link org.apache.pig.PigServer.Graph#compile(LogicalPlan)}.
     *
     * @throws org.apache.pig.impl.logicalLayer.FrontendException
     *
     */
    private void optimizeLogicalPlan() throws FrontendException {
        new ColumnAliasConversionVisitor(logicalPlan).visit();
        new SchemaAliasVisitor(logicalPlan).visit();
        new ScalarVisitor(logicalPlan, pigContext, currentScope).visit();

        CompilationMessageCollector collector = new CompilationMessageCollector();

        new TypeCheckingRelVisitor(logicalPlan, collector).visit();
        CompilationMessageCollector.logAllMessages(collector, log);

        new UnionOnSchemaSetter(logicalPlan).visit();
        new CastLineageSetter(logicalPlan, collector).visit();
        new ScalarVariableValidator(logicalPlan).visit();
    }

    public LogicalPlan getOptimizedLogicalPlan() throws ScriptCompilationException {
        if (optimizedLogicalPlan == null) {
            // Most logical plan optimizations are automatically applied when the physical plan is built.
            compilePhysicalPlan();
        }
        return optimizedLogicalPlan;
    }

    public PhysicalPlan getPhysicalPlan() throws ScriptCompilationException {
        if (physicalPlan == null) {
            compilePhysicalPlan();
        }
        return physicalPlan;
    }

    private void compilePhysicalPlan() throws ScriptCompilationException {
        try {
            // Make sure we have logical plan.
            LogicalPlan logicalPlan = getLogicalPlan();

            // Note: compiling the physical plan also applies optimizations to the logical plan! In order to keep the
            // un-optimized plan, we need to deep-clone it.
            this.logicalPlan = LogicalPlanUtil.clone(logicalPlan);
            optimizedLogicalPlan = logicalPlan;

            // Compile physical plan.
            physicalPlan = pigContext.getExecutionEngine().compile(optimizedLogicalPlan, null);
        } catch (IOException e) {
            throw new ScriptCompilationException("Failed to build physical plan.", e);
        }
    }

    public MROperPlan getMapReducePlan() throws ScriptCompilationException {
        if (mapReducePlan == null) {
            compileMapReducePlan();
        }
        return mapReducePlan;
    }

    private void compileMapReducePlan() throws ScriptCompilationException {
        try {
            // Make sure we have physical plan.
            PhysicalPlan physicalPlan = getPhysicalPlan();

            // Build map-reduce plan.
            MapRedUtil.checkLeafIsStore(physicalPlan, pigContext);
            MapReduceLauncher launcher = new MapReduceLauncher();
            mapReducePlan = launcher.compile(physicalPlan, pigContext);
        } catch (IOException e) {
            throw new ScriptCompilationException("Failed to build map-reduce plan.", e);
        }
    }
}

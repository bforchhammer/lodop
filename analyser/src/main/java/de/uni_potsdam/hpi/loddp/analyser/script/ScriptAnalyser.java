package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompilerException;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

public class ScriptAnalyser {
    protected static final Log log = LogFactory.getLog(ScriptAnalyser.class);
    private final PigContext pigContext;

    public ScriptAnalyser(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    public AnalysedScript analyse(PigScript script) throws ScriptCompilerException {
        ScriptCompiler scriptCompiler = new ScriptCompiler(pigContext, script, "fake-input.nq.gz", "fake-output");
        LogicalPlan logicalPlan = scriptCompiler.getLogicalPlan();
        LogicalPlan optimizedLogicalPlan = scriptCompiler.getOptimizedLogicalPlan();
        PhysicalPlan physicalPlan = scriptCompiler.getPhysicalPlan();
        MROperPlan mapReducePlan = scriptCompiler.getMapReducePlan();
        return new AnalysedScript(script, optimizedLogicalPlan, logicalPlan, physicalPlan, mapReducePlan);
    }
}

package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompilerException;
import de.uni_potsdam.hpi.loddp.common.printing.GraphvizDumper;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper around a pig script, which adds access to analytical information such as the parsed logical, physical and
 * mapreduce plans.
 */
public class AnalysedScript extends PigScript {
    private static final Log log = LogFactory.getLog(AnalysedScript.class);
    private PigScript script;
    private LogicalPlan logicalPlan;
    private LogicalPlan unoptimizedLogicalPlan;
    private PhysicalPlan physicalPlan;
    private MROperPlan mapReducePlan;
    private ScriptCompiler compiler;

    public AnalysedScript(PigScript script, ScriptCompiler compiler) {
        this.script = script;
        this.compiler = compiler;
    }

    public AnalysedScript(PigScript script, LogicalPlan logicalPlan, LogicalPlan unoptimizedLogicalPlan, PhysicalPlan physicalPlan, MROperPlan mapReducePlan) {
        this.script = script;
        this.logicalPlan = logicalPlan;
        this.unoptimizedLogicalPlan = unoptimizedLogicalPlan;
        this.physicalPlan = physicalPlan;
        this.mapReducePlan = mapReducePlan;
    }

    public LogicalPlan getLogicalPlan() {
        if (logicalPlan == null && compiler != null) {
            try {
                logicalPlan = compiler.getOptimizedLogicalPlan();
            } catch (ScriptCompilerException e) {
                log.error("Failed to retrieve optimized logical plan.", e);
            }
        }
        return logicalPlan;
    }

    public LogicalPlan getUnoptimizedLogicalPlan() {
        if (unoptimizedLogicalPlan == null && compiler != null) {
            try {
                unoptimizedLogicalPlan = compiler.getLogicalPlan();
            } catch (ScriptCompilerException e) {
                log.error("Failed to retrieve unoptimized logical plan.", e);
            }
        }
        return unoptimizedLogicalPlan;
    }

    public PhysicalPlan getPhysicalPlan() {
        if (physicalPlan == null && compiler != null) {
            try {
                physicalPlan = compiler.getPhysicalPlan();
            } catch (ScriptCompilerException e) {
                log.error("Failed to retrieve physical plan.", e);
            }
        }
        return physicalPlan;
    }

    public MROperPlan getMapReducePlan() {
        if (mapReducePlan == null && compiler != null) {
            try {
                mapReducePlan = compiler.getMapReducePlan();
            } catch (ScriptCompilerException e) {
                log.error("Failed to retrieve map-reduce plan.", e);
            }
        }
        return mapReducePlan;
    }

    @Override
    public InputStream getNewInputStream() {
        return script.getNewInputStream();
    }

    @Override
    public String getScriptName() {
        return script.getScriptName();
    }

    public void dumpPlansAsGraphs() throws IOException {
        GraphvizDumper dumper = new GraphvizDumper("dot/" + getScriptName() + "/");
        dumper.print(getUnoptimizedLogicalPlan(), "-unoptimized");
        dumper.print(getLogicalPlan(), "-optimized");
        dumper.print(getPhysicalPlan());
        dumper.print(getMapReducePlan());
    }
}

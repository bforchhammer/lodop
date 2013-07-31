package de.uni_potsdam.hpi.loddp.analyser.script;

import de.uni_potsdam.hpi.loddp.common.GraphvizHelper;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.DotPOPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.newplan.logical.DotLOPrinter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * Wrapper around a pig script, which adds access to analytical information such as the parsed logical, physical and
 * mapreduce plans.
 */
public class AnalysedScript extends PigScript {
    private PigScript script;
    private LogicalPlan logicalPlan;
    private PhysicalPlan physicalPlan;
    private MROperPlan mapReducePlan;

    public AnalysedScript(PigScript script, LogicalPlan logicalPlan, PhysicalPlan physicalPlan, MROperPlan mapReducePlan) {
        this.script = script;
        this.logicalPlan = logicalPlan;
        this.physicalPlan = physicalPlan;
        this.mapReducePlan = mapReducePlan;
    }

    public LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public MROperPlan getMapReducePlan() {
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
        dumpPlanAsGraph(logicalPlan);
        dumpPlanAsGraph(physicalPlan);
        dumpPlanAsGraph(mapReducePlan);
    }

    private File getDotOutputFile(String suffix) {
        return new File("dot/" + getScriptName() + "-" + suffix + ".dot");
    }

    private void dumpPlanAsGraph(Object plan) throws IOException {
        File dotFile;
        if (plan instanceof LogicalPlan) {
            dotFile = getDotOutputFile("logical");
        } else if (plan instanceof PhysicalPlan) {
            dotFile = getDotOutputFile("physical");
        } else if (plan instanceof MROperPlan) {
            dotFile = getDotOutputFile("mapreduce");
        } else {
            throw new IllegalArgumentException("Expected plan parameter to be an object of type LogicalPlan, " +
                "PhysicalPlan, or MROperPlan . Received " + plan.getClass().getName() + " instead.");
        }
        dumpPlanAsGraph(plan, dotFile);
    }

    private void dumpPlanAsGraph(Object plan, File dotFile) throws IOException {
        PrintStream ps = new PrintStream(dotFile);
        if (plan instanceof LogicalPlan) {
            //new DotPlanDumper(plan, new PrintStream(dotFile)).dump();
            new DotLOPrinter((LogicalPlan) plan, ps).dump();
        } else if (plan instanceof PhysicalPlan) {
            new DotPOPrinter((PhysicalPlan) plan, ps).dump();
        } else if (plan instanceof MROperPlan) {
            new DotMRPrinter((MROperPlan) plan, ps).dump();
        } else {
            throw new IllegalArgumentException("Expected plan parameter to be an object of type LogicalPlan, " +
                "PhysicalPlan, or MROperPlan . Received " + plan.getClass().getName() + " instead.");
        }
        GraphvizHelper.convertToImage("png", dotFile);
    }
}

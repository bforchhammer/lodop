package de.uni_potsdam.hpi.loddp.common.printing;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.DotPOPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.PlanDumper;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Dumps plans of operators as graph images.
 */
public class GraphvizDumper {
    protected static final Map<Class, String> planTypes;
    protected static final Map<Class, Class> planDumpers;
    private File outputDirectory;
    private boolean verbose = false;
    private boolean deleteDotFile = true;
    private String filenamePrefix;

    static {
        planTypes = new HashMap<Class, String>();
        planTypes.put(LogicalPlan.class, "logical");
        planTypes.put(PhysicalPlan.class, "physical");
        planTypes.put(MROperPlan.class, "mapreduce");

        planDumpers = new HashMap<Class, Class>();
        planDumpers.put(LogicalPlan.class, LogicalPlanPrinter.class);
        planDumpers.put(PhysicalPlan.class, DotPOPrinter.class);
        planDumpers.put(MROperPlan.class, DotMRPrinter.class);
    }

    public GraphvizDumper(String outputDirectory) {
        this(new File(outputDirectory));
    }

    public GraphvizDumper(File outputDirectory) {
        setOutputDirectory(outputDirectory);
    }

    public File getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(File outputDirectory) {
        if (!(outputDirectory.exists() || outputDirectory.mkdirs())) {
            throw new RuntimeException("Failed to create output directories.");
        }
        if (!outputDirectory.isDirectory()) {
            throw new IllegalArgumentException("The given output directory is not actually a directory.");
        }
        this.outputDirectory = outputDirectory;
    }

    public boolean outputExists() {
        return outputDirectory.exists();
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setDeleteDotFile(boolean deleteDotFile) {
        this.deleteDotFile = deleteDotFile;
    }

    public void setFilenamePrefix(String filenamePrefix) {
        this.filenamePrefix = filenamePrefix;
    }

    public void setPlanDumper(Class operatorPlanClass, Class planDumperClass) {
        planDumpers.put(operatorPlanClass, planDumperClass);
    }

    public void print(Object operatorPlan, String suffix) {
        String planType = getPlanType(operatorPlan);
        File dotFile = getDotFile(planType, suffix);
        try {
            dumpGraph(operatorPlan, new PrintStream(dotFile));
            GraphvizUtil.convertToImage("png", dotFile, deleteDotFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void print(Object operatorPlan) {
        print(operatorPlan, null);
    }

    protected String getPlanType(Object operatorPlan) {
        for (Map.Entry<Class, String> entry : planTypes.entrySet()) {
            @SuppressWarnings("unchecked")
            boolean assignable = entry.getKey().isAssignableFrom(operatorPlan.getClass());
            if (entry.getKey().isInstance(operatorPlan) || assignable) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("Expected plan parameter to be an object of type LogicalPlan, " +
            "PhysicalPlan, or MROperPlan . Received " + operatorPlan.getClass().getName() + " instead.");
    }

    protected Class getPlanDumperClass(Object operatorPlan) {
        for (Map.Entry<Class, Class> entry : planDumpers.entrySet()) {
            @SuppressWarnings("unchecked")
            boolean assignable = entry.getKey().isAssignableFrom(operatorPlan.getClass());
            if (entry.getKey().isInstance(operatorPlan) || assignable) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("Expected plan parameter to be an object of type LogicalPlan, " +
            "PhysicalPlan, or MROperPlan . Received " + operatorPlan.getClass().getName() + " instead.");
    }

    protected File getDotFile(String planType, String filenameSuffix) {
        StringBuilder filename = new StringBuilder();
        if (filenamePrefix != null) filename.append(filenamePrefix);
        if (planType != null) filename.append(planType);
        else filename.append("unknown");
        if (filenameSuffix != null) filename.append(filenameSuffix);
        filename.append(".dot");
        return new File(outputDirectory, filename.toString());
    }

    protected <T> T getPlanDumperInstance(Class<? extends T> printerClass, Class[] argumentTypes,
                                          Object... arguments) {
        try {
            Constructor<? extends T> ctr = printerClass.getConstructor(argumentTypes);
            return ctr.newInstance(arguments);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create instance of printer class.", e);
        }
    }

    protected void dumpGraph(Object operatorPlan, PrintStream ps) {
        Class dumperClass = getPlanDumperClass(operatorPlan);

        // "newplan" implementation -> logical plan printers.
        if (PlanDumper.class.isAssignableFrom(dumperClass)) {
            PlanDumper dumper = getPlanDumperInstance((Class<? extends PlanDumper>) dumperClass,
                new Class[] {BaseOperatorPlan.class, PrintStream.class}, operatorPlan, ps);
            dumper.setVerbose(verbose);
            dumper.dump();
        }
        // "old" implementation -> physicla and mapreduce plan printers.
        else if (org.apache.pig.impl.plan.PlanDumper.class.isAssignableFrom(dumperClass)) {
            org.apache.pig.impl.plan.PlanDumper dumper = getPlanDumperInstance((Class<? extends org.apache.pig.impl.plan.PlanDumper>) dumperClass,
                new Class[] {operatorPlan.getClass(), PrintStream.class}, operatorPlan, ps);
            dumper.setVerbose(verbose);
            dumper.dump();
        }
        // unknown implementation.
        else {
            throw new RuntimeException("Printer class does not belong to a known interface.");
        }
    }

}


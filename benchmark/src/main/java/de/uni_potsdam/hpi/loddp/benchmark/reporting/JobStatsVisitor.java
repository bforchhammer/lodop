package de.uni_potsdam.hpi.loddp.benchmark.reporting;


import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.*;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;

import java.io.IOException;
import java.util.*;

public class JobStatsVisitor extends PlanVisitor {
    Map<JobID, TaskPhaseStatistics> cleanupStatistics = new HashMap<JobID, TaskPhaseStatistics>();
    Map<JobID, TaskPhaseStatistics> setupStatistics = new HashMap<JobID, TaskPhaseStatistics>();
    TaskPhaseStatistics totalMapReduceStatistics = new TaskPhaseStatistics("MapReduce total");
    private JobClient jobClient;
    private int numberMapsTotal = 0;
    private int numberReducesTotal = 0;
    // Total time in terms of additive "time spent for computation"; these can be higher than actual duration because
    // of parallelism.
    private long totalMapTime = 0;
    private long totalReduceTime = 0;
    private long totalSetupTime = 0;
    private long totalCleanupTime = 0;
    private long totalPigTime = 0;
    private long totalMapReduceTime = 0;
    // Duration in terms of "real time", i.e. parallelism
    private long overallDuration = 0;
    private long mapReduceDuration = 0;

    private JobStatsVisitor(PigStats pigStats, PlanWalker walker) throws FrontendException {
        super(pigStats.getJobGraph(), walker);
        this.jobClient = pigStats.getJobClient();
        this.overallDuration = pigStats.getDuration();
    }

    protected static JobStatsVisitor createInstance(PigStats pigStats) throws FrontendException {
        PlanWalker walker = new JobGraphWalker(pigStats.getJobGraph());
        return new JobStatsVisitor(pigStats, walker);
    }

    public int getNumberMapsTotal() {
        return numberMapsTotal;
    }

    public int getNumberReducesTotal() {
        return numberReducesTotal;
    }

    public long getTotalPigTime() {
        // Total pig time is time between operators (which is being summed up by visit() during plan walking),
        // plus the time before and after Hadoop does anything, i.e overall duration minus Hadoop duration.
        return totalPigTime + (overallDuration - getMapReduceDuration());
    }

    public long getMapReduceDuration() {
        mapReduceDuration = totalMapReduceStatistics.getDuration();
        return mapReduceDuration;
    }

    public long getTotalMapTime() {
        return totalMapTime;
    }

    public long getTotalReduceTime() {
        return totalReduceTime;
    }

    public long getTotalSetupTime() {
        return totalSetupTime;
    }

    public long getTotalCleanupTime() {
        return totalCleanupTime;
    }

    public long getTotalMapReduceTime() {
        totalMapReduceTime = totalSetupTime + totalMapTime + totalReduceTime + totalCleanupTime;
        return totalMapReduceTime;
    }

    protected TaskPhaseStatistics getSetupStatistics(JobID jobID) throws FrontendException {
        if (!setupStatistics.containsKey(jobID)) {
            try {
                TaskPhaseStatistics statistics = TaskPhaseStatistics.getTaskPhaseStatistics("setup",
                    jobClient.getSetupTaskReports(jobID));
                totalMapReduceStatistics.update(statistics);
                setupStatistics.put(jobID, statistics);
            } catch (IOException e) {
                throw new FrontendException("Failed to grab task reports for something.", e);
            }
        }
        return setupStatistics.get(jobID);
    }

    protected TaskPhaseStatistics getSetupStatistics(JobStats jobStats) throws FrontendException {
        JobID jobId = JobID.forName(jobStats.getJobId());
        return getSetupStatistics(jobId);
    }

    protected TaskPhaseStatistics getCleanupStatistics(JobID jobID) throws FrontendException {
        if (!cleanupStatistics.containsKey(jobID)) {
            try {
                TaskPhaseStatistics statistics = TaskPhaseStatistics.getTaskPhaseStatistics("cleanup",
                    jobClient.getCleanupTaskReports(jobID));
                totalMapReduceStatistics.update(statistics);
                cleanupStatistics.put(jobID, statistics);
            } catch (IOException e) {
                throw new FrontendException("Failed to grab task reports for something.", e);
            }
        }
        return cleanupStatistics.get(jobID);
    }

    protected TaskPhaseStatistics getCleanupStatistics(JobStats jobStats) throws FrontendException {
        JobID jobId = JobID.forName(jobStats.getJobId());
        return getCleanupStatistics(jobId);
    }

    public void visit(JobStats js) throws FrontendException {
        if (js.isSuccessful()) {
            numberMapsTotal += js.getNumberMaps();
            numberReducesTotal += js.getNumberReduces();

            totalMapTime += js.getMaxMapTime();
            totalReduceTime += js.getMaxReduceTime();
            totalSetupTime += getSetupStatistics(js).getDuration();
            totalCleanupTime += getCleanupStatistics(js).getDuration();

            // Calculcate pig time between operators.
            List<Operator> predecessors = plan.getPredecessors(js);
            if (predecessors != null) {
                long pigEndTime = getSetupStatistics(js).getStartTime();
                TaskPhaseStatistics predecessorStatistics = new TaskPhaseStatistics("predecessor cleanups");
                for (Operator predecessor : predecessors) {
                    JobStats pred = (JobStats) predecessor;
                    if (pred.isSuccessful()) {
                        predecessorStatistics.update(getCleanupStatistics(pred));
                    }
                }
                totalPigTime += (pigEndTime - predecessorStatistics.getFinishTime());
            }
        }
    }

    protected static class JobGraphWalker extends DependencyOrderWalker {
        public JobGraphWalker(OperatorPlan plan) {
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
                // Default way of handling things
                op.accept(visitor);

                // Work-around for JobStatsVisitor
                if (op instanceof JobStats && visitor instanceof JobStatsVisitor) {
                    ((JobStatsVisitor) visitor).visit((JobStats) op);
                }
            }
        }
    }
}

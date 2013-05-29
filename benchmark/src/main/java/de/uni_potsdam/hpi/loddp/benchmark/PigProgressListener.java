package de.uni_potsdam.hpi.loddp.benchmark;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

public class PigProgressListener implements PigProgressNotificationListener {
    @Override
    public void initialPlanNotification(String scriptId, MROperPlan plan) {
        System.out.println(scriptId + ": initialPlanNotification");
    }

    @Override
    public void launchStartedNotification(String scriptId, int numJobsToLaunch) {
        System.out.println(scriptId + ": launched " + numJobsToLaunch + " job");
    }

    @Override
    public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) {
        System.out.println(scriptId + ": submitted " + numJobsSubmitted + " job");
    }

    @Override
    public void jobStartedNotification(String scriptId, String assignedJobId) {
        System.out.println(scriptId + ": started job " + assignedJobId);
    }

    @Override
    public void jobFinishedNotification(String scriptId, JobStats jobStats) {
        System.out.println(scriptId + ": finished job, got a jobstats object");
    }

    @Override
    public void jobFailedNotification(String scriptId, JobStats jobStats) {
        System.out.println(scriptId + ": job failed. got a jobstats object");
    }

    @Override
    public void outputCompletedNotification(String scriptId, OutputStats outputStats) {
        System.out.println(scriptId + ": completed, got an outputstats");
    }

    @Override
    public void progressUpdatedNotification(String scriptId, int progress) {
    }

    @Override
    public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
        System.out.println(scriptId + ": completed;" + numJobsSucceeded + " jobs succeeded");
    }
}

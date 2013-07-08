package de.uni_potsdam.hpi.loddp.benchmark.execution;

import java.util.Properties;

public enum HadoopLocation {

    LOCALHOST("hdfs://localhost:9000", "localhost:9001"),
    HPI_CLUSTER("hdfs://tenemhead2.hpi.uni-potsdam.de", "tenemhead2.hpi.uni-potsdam.de:9001");
    private String fsDefaultName;
    private String mapredJobtracker;

    private HadoopLocation(String fsDefaultName, String mapredJobtracker) {
        this.fsDefaultName = fsDefaultName;
        this.mapredJobtracker = mapredJobtracker;
    }

    public void setProperties(Properties properties) {
        properties.setProperty("fs.default.name", fsDefaultName);
        properties.setProperty("mapred.job.tracker", mapredJobtracker);
    }

    public String getFsDefaultName() {
        return fsDefaultName;
    }

    public String getMapredJobtracker() {
        return mapredJobtracker;
    }
}

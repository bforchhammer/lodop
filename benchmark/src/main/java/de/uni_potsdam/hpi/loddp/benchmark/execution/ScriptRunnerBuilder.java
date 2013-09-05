package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.common.HadoopLocation;
import de.uni_potsdam.hpi.loddp.common.execution.BasePigRunner;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ScriptRunnerBuilder {
    private static final Log log = LogFactory.getLog(ScriptRunnerBuilder.class);
    private HadoopLocation location = HadoopLocation.LOCALHOST;
    private int repeat = 1;
    private String hdfsOutputDirectory = "";
    private boolean merged = false;
    private boolean replaceExistingResults = true;

    public void setLocation(HadoopLocation location) {
        this.location = location;
    }

    public void setRepeat(int repeat) {
        if (repeat < 1) {
            log.warn("--repeat argument ignored because the specified value was negative or zero (positive " +
                "integer expected).");
        } else {
            this.repeat = repeat;
            log.info(String.format("Repeating each script %d times.", repeat));
        }
    }

    public void setHdfsOutputDirectory(String hdfsOutputDirectory) {
        this.hdfsOutputDirectory = normalizePath(hdfsOutputDirectory);
    }

    private String normalizePath(String path) {
        if (path == null || path.isEmpty()) return "";
        if (!path.endsWith("/")) path += '/';
        return path;
    }

    public void setMerged(boolean merged) {
        this.merged = merged;
        if (merged) {
            log.info("Scripts are merged into one large plan.");
        }
    }

    public void setReplaceExistingResults(boolean replaceExistingResults) {
        this.replaceExistingResults = replaceExistingResults;
    }

    private PigScriptRunner buildPigRunner() {
        PigRunner runner = new BasePigRunner(location);
        runner.setReplaceExistingResults(replaceExistingResults);
        if (repeat > 1) {
            return new RepeatedPigScriptRunner(runner, repeat);
        }
        return new PigScriptRunner(runner);
    }

    public ScriptRunner build() {
        PigScriptRunner pigScriptRunner = buildPigRunner();
        ScriptRunner scriptRunner;
        if (merged) {
            scriptRunner = new MergedScriptRunner(pigScriptRunner, hdfsOutputDirectory);
        } else {
            scriptRunner = new DefaultScriptRunner(pigScriptRunner, hdfsOutputDirectory);
        }
        return scriptRunner;
    }
}

package de.uni_potsdam.hpi.loddp.benchmark.execution;

import de.uni_potsdam.hpi.loddp.benchmark.reporting.ScriptStats;
import de.uni_potsdam.hpi.loddp.common.execution.PigRunnerException;
import de.uni_potsdam.hpi.loddp.common.execution.ScriptCompiler;
import de.uni_potsdam.hpi.loddp.common.scripts.PigScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Abstract base class for {@link ScriptRunner script runners}.
 */
public abstract class AbstractScriptRunner implements ScriptRunner {
    private static final Log log = LogFactory.getLog(AbstractScriptRunner.class);
    protected final String hdfsOutputDirectory;
    protected PigScriptRunner pigScriptRunner;

    /**
     * Constructor.
     *
     * @param pigScriptRunner
     * @param hdfsOutputDirectory
     */
    protected AbstractScriptRunner(PigScriptRunner pigScriptRunner, String hdfsOutputDirectory) {
        this.pigScriptRunner = pigScriptRunner;
        this.hdfsOutputDirectory = hdfsOutputDirectory;
    }

    /**
     * Build input + output filenames and initialise a respective script compiler.
     *
     * @param script
     * @param file
     *
     * @return
     *
     * @throws PigRunnerException
     */
    protected ScriptCompiler getCompiler(PigScript script, InputFile file) throws PigRunnerException {
        return new ScriptCompiler(pigScriptRunner.getPigContext(), script, getInputFilename(file),
            getOutputFilename(script, file));
    }

    protected String getInputFilename(InputFile file) {
        return file.getFilename();
    }

    protected String getOutputFilename(PigScript script, InputFile file) {
        return hdfsOutputDirectory + "results-" + file.getFileSetIdentifier() + "-" +
            file.getTupleCount() + "/" + script.getScriptFileName();
    }

    /**
     * Execute the script contained within the given ScriptCompiler.
     *
     * @param compiler
     * @param scriptName
     * @param file
     *
     * @return
     *
     * @throws PigRunnerException
     */
    protected ScriptStats execute(ScriptCompiler compiler, String scriptName,
                                  InputFile file) throws PigRunnerException {
        return pigScriptRunner.execute(compiler, scriptName, file);
    }
}

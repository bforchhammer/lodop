package de.uni_potsdam.hpi.loddp.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.PropertiesUtil;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

public class PigContextUtil {
    protected static final Log log = LogFactory.getLog(PigContextUtil.class);
    private static boolean useCustomReducerEstimator = false;

    /**
     * Creates a new PigContext running in LOCAL mode.
     */
    public static PigContext getContext() throws IOException {
        return getContext(ExecType.LOCAL, getProperties());
    }

    /**
     * Creates a new PigContext running in MAPREDUCE mode, with connection properties set depending on the given hadoop
     * location.
     */
    public static PigContext getContext(HadoopLocation type) throws IOException {
        return getContext(ExecType.MAPREDUCE, getProperties(type));
    }

    /**
     * Creates a new PigContext with the given mode and properties.
     */
    protected static PigContext getContext(ExecType type, Properties properties) throws IOException {
        PigContext pigContext = new PigContext(type, properties);

        // Register jars which contain UDFs.
        registerJar(pigContext, "pig-udfs/loddp-udf.jar");
        registerJar(pigContext, "pig-udfs/ldif-single-0.5.1-jar-with-dependencies.jar");
        registerJar(pigContext, "pig-udfs/loddesc-core-0.1.jar");
        registerJar(pigContext, "pig-udfs/piggybank.jar");

        return pigContext;
    }

    protected static Properties getProperties() {
        Properties properties = PropertiesUtil.loadDefaultProperties();

        if (useCustomReducerEstimator) {
            // Register custom reducer estimator.
            properties.setProperty("pig.exec.reducer.estimator", "de.uni_potsdam.hpi.loddp.common.optimization.OperationAwareReducerEstimator");
        }

        return properties;
    }

    /**
     * Creates a properties instance with hadoop cluster configuration settings depending on the given Hadoop location.
     *
     * @param type The server address (cluster or local).
     *
     * @return A properties object with at least the following two properties filled in: "fs.default.name",
     *         "mapred.job.tracker".
     */
    protected static Properties getProperties(HadoopLocation type) {
        Properties properties = getProperties();
        type.setProperties(properties);
        return properties;
    }

    /**
     * Registers a jar file. Name of the jar file can be an absolute or relative path.
     *
     * If multiple resources are found with the specified name, the first one is registered as returned by
     * getSystemResources. A warning is issued to inform the user.
     *
     * @param name of the jar file to register
     *
     * @throws IOException
     */
    public static void registerJar(PigContext context, String name) throws IOException {
        // first try to locate jar via system resources
        // if this fails, try by using "name" as File (this preserves
        // compatibility with case when user passes absolute path or path
        // relative to current working directory.)
        if (name != null) {
            URL resource = locateJarFromResources(name);

            if (resource == null) {
                FileLocalizer.FetchFileRet[] files = FileLocalizer.fetchFiles(context.getProperties(), name);

                for (FileLocalizer.FetchFileRet file : files) {
                    File f = file.file;
                    if (!f.canRead()) {
                        int errCode = 4002;
                        String msg = "Can't read jar file: " + name;
                        throw new FrontendException(msg, errCode, PigException.USER_ENVIRONMENT);
                    }

                    context.addJar(f.toURI().toURL());
                }
            } else {
                context.addJar(resource);
            }
        }
    }

    private static URL locateJarFromResources(String jarName) throws IOException {
        Enumeration<URL> urls = ClassLoader.getSystemResources(jarName);
        URL resourceLocation = null;

        if (urls.hasMoreElements()) {
            resourceLocation = urls.nextElement();
        }

        if (urls.hasMoreElements()) {
            StringBuffer sb = new StringBuffer("Found multiple resources that match ");
            sb.append(jarName).append(": ").append(resourceLocation);
            while (urls.hasMoreElements()) {
                sb.append(urls.nextElement()).append("; ");
            }
            log.info(sb.toString());
        }

        return resourceLocation;
    }

    public static void useCustomReducerEstimator() {
        useCustomReducerEstimator = true;
    }

}

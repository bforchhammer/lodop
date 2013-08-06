package de.uni_potsdam.hpi.loddp.common;

import org.apache.pig.newplan.logical.relational.LogicalPlan;

public class LogicalPlanUtil {
    public static LogicalPlan clone(LogicalPlan original) {
        LogicalPlanCloner cloner = new LogicalPlanCloner();
        return cloner.deepClone(original);
    }

    protected static class LogicalPlanCloner extends com.rits.cloning.Cloner {
        public LogicalPlanCloner() {
            dontClone(
                org.apache.pig.parser.SourceLocation.class,
                org.apache.hadoop.conf.Configuration.class
            );
            //setDumpClonedClasses(true); // for debugging only
        }

        @Override
        protected boolean considerImmutable(Class<?> clz) {
            if (clz.getName().startsWith("org.apache.commons.logging")) return true;
            if (clz.getName().startsWith("org.apache.log4j")) return true;
            if (clz.getName().startsWith("org.slf4j")) return true;
            if (clz.getName().startsWith("java.util.logging")) return true;
            return super.considerImmutable(clz);
        }
    }
}

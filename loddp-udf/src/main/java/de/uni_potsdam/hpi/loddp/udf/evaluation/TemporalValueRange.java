package de.uni_potsdam.hpi.loddp.udf.evaluation;

import de.uni_potsdam.hpi.loddp.udf.util.TemporalHelper;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.joda.time.ReadableDuration;
import org.joda.time.ReadableInstant;
import org.joda.time.ReadablePartial;

import java.io.IOException;
import java.util.Iterator;

/**
 * Accepts a full RDF object, i.e. :bag (ntype, value, dttype).
 */
public class TemporalValueRange extends EvalFunc<String> implements Accumulator<String> {

    private ReadableDuration minDuration = null;
    private ReadableDuration maxDuration = null;
    private ReadableInstant minInstant = null;
    private ReadableInstant maxInstant = null;
    private ReadablePartial minPartial = null;
    private ReadablePartial maxPartial = null;

    private static String getValue(ReadableDuration minDuration, ReadableDuration maxDuration, ReadableInstant minInstant, ReadableInstant maxInstant, ReadablePartial minPartial, ReadablePartial maxPartial) {
        String duration = rangeString(minDuration, maxDuration);
        String instant = rangeString(minInstant, maxInstant);
        String partial = rangeString(minPartial, maxPartial);
        int c = 0;
        StringBuilder sb = new StringBuilder();
        if (duration != null) {
            sb.append(duration);
            c++;
        }
        if (instant != null) {
            if (c != 0) sb.append(" | ");
            sb.append(instant);
            c++;
        }
        if (partial != null) {
            if (c != 0) sb.append(" | ");
            sb.append(partial);
            c++;
        }
        if (c > 1) {
            sb.append(" | (Mixed DataTypes!)");
        }
        return sb.toString();
    }

    private static String rangeString(Object min, Object max) {
        if (min == null || max == null) {
            return null;
        }
        return String.format("%s - %s", min.toString(), max.toString());
    }

    private Comparable getTemporalValue(Tuple input) throws IOException {
        // Crazy stuff (looks like each tuple is wrapped in another bag?)
        Tuple tuple = (Tuple) input.get(0);
        String valueStr = (String) (tuple.get(1));
        String typeStr = (String) (tuple.get(2));
        try {
            return TemporalHelper.parseTemporalValue(typeStr, valueStr);
        } catch (Exception e) {
            warn("Error: cannot parse " + tuple.toString() + ". " + e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    @Override
    public void accumulate(Tuple b) throws IOException {
        DataBag values = (DataBag) b.get(0);
        if (values.size() == 0) {
            return;
        }

        for (Iterator<Tuple> it = values.iterator(); it.hasNext(); ) {
            Tuple tuple = it.next();
            Comparable temporalValue = getTemporalValue(tuple);
            if (temporalValue instanceof ReadableDuration) {
                ReadableDuration t = (ReadableDuration) temporalValue;
                if (minDuration == null || maxDuration == null) {
                    minDuration = maxDuration = t;
                } else {
                    if (t.compareTo(minDuration) < 0) minDuration = t;
                    if (t.compareTo(maxDuration) > 0) maxDuration = t;
                }
            } else if (temporalValue instanceof ReadableInstant) {
                ReadableInstant t = (ReadableInstant) temporalValue;
                if (minInstant == null || maxInstant == null) {
                    minInstant = maxInstant = t;
                } else {
                    if (t.compareTo(minInstant) < 0) minInstant = t;
                    if (t.compareTo(maxInstant) > 0) maxInstant = t;
                }
            } else if (temporalValue instanceof ReadablePartial) {
                ReadablePartial t = (ReadablePartial) temporalValue;
                if (minPartial == null || maxPartial == null) {
                    minPartial = maxPartial = t;
                } else {
                    if (t.compareTo(minPartial) < 0) minPartial = t;
                    if (t.compareTo(maxPartial) > 0) maxPartial = t;
                }
            }
        }
    }

    @Override
    public String getValue() {
        return getValue(minDuration, maxDuration, minInstant, maxInstant, minPartial, maxPartial);
    }

    @Override
    public void cleanup() {
        minDuration = null;
        maxDuration = null;
        minInstant = null;
        maxInstant = null;
        minPartial = null;
        maxPartial = null;
    }

    @Override
    public String exec(Tuple input) throws IOException {
        DataBag values = (DataBag) input.get(0);
        if (values.size() == 0) {
            return null;
        }
        ReadableDuration minDuration = null;
        ReadableDuration maxDuration = null;
        ReadableInstant minInstant = null;
        ReadableInstant maxInstant = null;
        ReadablePartial minPartial = null;
        ReadablePartial maxPartial = null;

        for (Iterator<Tuple> it = values.iterator(); it.hasNext(); ) {
            Tuple tuple = it.next();
            Comparable temporalValue = getTemporalValue(tuple);
            if (temporalValue instanceof ReadableDuration) {
                ReadableDuration t = (ReadableDuration) temporalValue;
                if (minDuration == null || maxDuration == null) {
                    minDuration = maxDuration = t;
                } else {
                    if (t.compareTo(minDuration) < 0) minDuration = t;
                    if (t.compareTo(maxDuration) > 0) maxDuration = t;
                }
            } else if (temporalValue instanceof ReadableInstant) {
                ReadableInstant t = (ReadableInstant) temporalValue;
                if (minInstant == null || maxInstant == null) {
                    minInstant = maxInstant = t;
                } else {
                    if (t.compareTo(minInstant) < 0) minInstant = t;
                    if (t.compareTo(maxInstant) > 0) maxInstant = t;
                }
            } else if (temporalValue instanceof ReadablePartial) {
                ReadablePartial t = (ReadablePartial) temporalValue;
                if (minPartial == null || maxPartial == null) {
                    minPartial = maxPartial = t;
                } else {
                    if (t.compareTo(minPartial) < 0) minPartial = t;
                    if (t.compareTo(maxPartial) > 0) maxPartial = t;
                }
            }
        }

        return getValue(minDuration, maxDuration, minInstant, maxInstant, minPartial, maxPartial);
    }
}

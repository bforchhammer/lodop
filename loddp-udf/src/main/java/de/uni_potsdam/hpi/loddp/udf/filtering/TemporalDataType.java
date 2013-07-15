package de.uni_potsdam.hpi.loddp.udf.filtering;

import de.uni_potsdam.hpi.loddp.udf.util.TemporalHelper;
import org.apache.pig.FilterFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class TemporalDataType extends FilterFunc {

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return false;
        }
        String str = (String) input.get(0);

        // Check if the given string matches a built-in XSD date/time data-type.
        return TemporalHelper.isTemporalDataType(str);
    }
}

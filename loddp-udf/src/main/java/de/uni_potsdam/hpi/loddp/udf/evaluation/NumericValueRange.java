package de.uni_potsdam.hpi.loddp.udf.evaluation;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

public class NumericValueRange extends EvalFunc<String> implements Accumulator<String> {

    private BigDecimal intermediateMin = null;
    private BigDecimal intermediateMax = null;

    @Override
    public void accumulate(Tuple b) throws IOException {
        DataBag values = (DataBag) b.get(0);
        if (values.size() == 0) {
            return;
        }

        for (Iterator<Tuple> it = values.iterator(); it.hasNext(); ) {
            String str = (String) (it.next().get(0));
            if (NumberUtils.isNumber(str)) {
                BigDecimal number = NumberUtils.createBigDecimal(str);
                intermediateMin = intermediateMin == null ? number : intermediateMin.min(number);
                intermediateMax = intermediateMax == null ? number : intermediateMax.max(number);
            }
        }
    }

    @Override
    public String getValue() {
        return rangeString(intermediateMin, intermediateMax);
    }

    @Override
    public void cleanup() {
        intermediateMin = null;
        intermediateMax = null;
    }

    private String rangeString(BigDecimal min, BigDecimal max) {
        if (min == null || max == null) {
            return null;
        }
        return String.format("%s - %s", min.toString(), max.toString());
    }

    @Override
    public String exec(Tuple input) throws IOException {
        DataBag values = (DataBag) input.get(0);
        if (values.size() == 0) {
            return null;
        }
        BigDecimal curMin = null;
        BigDecimal curMax = null;

        for (Iterator<Tuple> it = values.iterator(); it.hasNext(); ) {
            String str = (String) (it.next().get(0));
            if (NumberUtils.isNumber(str)) {
                BigDecimal number = NumberUtils.createBigDecimal(str);
                curMin = curMin == null ? number : curMin.min(number);
                curMax = curMax == null ? number : curMax.max(number);
            }
        }

        return rangeString(curMin, curMax);
    }
}

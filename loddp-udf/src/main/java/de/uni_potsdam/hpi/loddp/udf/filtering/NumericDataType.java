package de.uni_potsdam.hpi.loddp.udf.filtering;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NumericDataType extends FilterFunc {

    private static final Set<String> numericTypes = new HashSet<String>();

    static {
        numericTypes.add("http://www.w3.org/2001/XMLSchema#int");
        //numericTypes.add("http://www.w3.org/2001/XMLSchema#date");
        numericTypes.add("http://dbpedia.org/datatype/second");
        numericTypes.add("http://www.w3.org/2001/XMLSchema#double");
        numericTypes.add("http://www.w3.org/2001/XMLSchema#integer");
        // @todo use schema information instead (?)
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return false;
        }
        String str = (String) input.get(0);
        return str != null && numericTypes.contains(str);
    }
}

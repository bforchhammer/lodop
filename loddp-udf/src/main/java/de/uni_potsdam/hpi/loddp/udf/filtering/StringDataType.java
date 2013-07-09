package de.uni_potsdam.hpi.loddp.udf.filtering;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Accepts tuples which match any built-in data type derived from xs:string, as defined in the XMLSchema document.
 *
 * @link http://www.w3.org/TR/xmlschema-2/
 */
public class StringDataType extends FilterFunc {

    private static final Set<String> stringTypes = new HashSet<String>();

    static {
        // XMLSchema simple types:
        stringTypes.add("http://www.w3.org/2001/XMLSchema#string");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#normalizedString");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#token");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#language");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#Name");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#NCName");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#NMTOKEN");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#NMTOKENS");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#ID");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#IDREF");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#ENTITY");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#IDREFS");
        stringTypes.add("http://www.w3.org/2001/XMLSchema#ENTITIES");
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return false;
        }
        String str = (String) input.get(0);
        return str != null && stringTypes.contains(str);
    }
}

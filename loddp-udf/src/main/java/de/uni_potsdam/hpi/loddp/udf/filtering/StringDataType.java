package de.uni_potsdam.hpi.loddp.udf.filtering;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLRuntimeException;
import org.semanticweb.owlapi.vocab.OWL2Datatype;

import java.io.IOException;

/**
 * Accepts tuples which match any built-in data type derived from xs:string, as defined in the XMLSchema document.
 *
 * @link http://www.w3.org/TR/xmlschema-2/
 */
public class StringDataType extends FilterFunc {

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return false;
        }
        String str = (String) input.get(0);

        // If the given string is empty for some reason, it's definitely not a string. ;-)
        if (str == null) {
            return false;
        }

        // Check if the given string matches a built-in string data-type.
        try {
            OWL2Datatype dt = OWL2Datatype.getDatatype(IRI.create(str));
            switch (dt.getCategory()) {
                case STRING_WITH_LANGUAGE_TAG:
                case STRING_WITHOUT_LANGUAGE_TAG:
                case UNIVERSAL:
                    return true;
                default:
                    return false;
            }
        } catch (OWLRuntimeException e) {
            // Not a built in datatype.
        }

        // If none of the checks above worked, it's probably not a string.
        return false;
    }
}

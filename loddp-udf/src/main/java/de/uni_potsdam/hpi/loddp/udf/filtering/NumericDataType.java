package de.uni_potsdam.hpi.loddp.udf.filtering;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLRuntimeException;
import org.semanticweb.owlapi.vocab.OWL2Datatype;

import java.io.IOException;

/**
 * Accepts tuples which represent a numeric data-type.
 *
 * Numeric data-types are strings which match one of the built-in numeric data types, as defined in the XMLSchema
 * document. This means any simple data types matching or derived from xs:double, xs:float, or xs:decimal.
 *
 * Numeric DBPedia properties (e.g. http://dbpedia.org/datatype/kilogram) are ignored because they are not properly
 * defined in any schema or ontology at the moment.
 *
 * @link http://mappings.dbpedia.org/server/ontology/dbpedia.owl
 * @link http://www.w3.org/TR/xmlschema-2/
 */
public class NumericDataType extends FilterFunc {

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return false;
        }
        String str = (String) input.get(0);

        // If the given string is empty for some reason, it's definitely not numeric. ;-)
        if (str == null) {
            return false;
        }

        // Check if the given string matches a built-in numeric data-type.
        try {
            OWL2Datatype dt = OWL2Datatype.getDatatype(IRI.create(str));
            return dt.isNumeric();
        } catch (OWLRuntimeException e) {
            // Not a built in datatype.
        }

        // If none of the checks above worked, it's probably not numeric.
        return false;
    }
}

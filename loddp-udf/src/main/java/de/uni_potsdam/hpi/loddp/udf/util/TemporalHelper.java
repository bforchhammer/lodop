package de.uni_potsdam.hpi.loddp.udf.util;

import org.apache.commons.lang.NotImplementedException;
import org.joda.time.*;
import org.joda.time.format.ISODateTimeFormat;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.vocab.XSDVocabulary;

import java.util.HashMap;
import java.util.Map;

public class TemporalHelper {

    private static final Map<String, XSDVocabulary> xslDataTypes;

    static {
        xslDataTypes = new HashMap<String, XSDVocabulary>();
        XSDVocabulary[] xsdTypes = XSDVocabulary.values();
        for (XSDVocabulary type : xsdTypes) {
            xslDataTypes.put(type.getShortName(), type);
        }
    }

    private static XSDVocabulary getXSDType(String str) {
        if (str == null) return null;
        String fragment = IRI.create(str).getFragment();
        return xslDataTypes.get(fragment);
    }

    /**
     * Checks whether the given string matches the URI of an temporal XSD type (e.g. http://www.w3
     * .org/2001/XMLSchema#dateTime).
     *
     * @param str The string representing an XSD type.
     *
     * @return TRUE if the given string matches an XSD temporal type, FALSE otherwise.
     */
    public static boolean isTemporalDataType(String str) {
        XSDVocabulary xsType = getXSDType(str);
        if (xsType == null) return false;
        switch (xsType) {
            case DATE:
            case DATE_TIME:
            case DATE_TIME_STAMP:
            case DURATION:
            case G_DAY:
            case G_MONTH:
            case G_MONTH_DAY:
            case G_YEAR:
            case G_YEAR_MONTH:
            case TIME:
                return true;
            default:
                return false;
        }
    }

    /**
     * Tries to parse the given string value into a yoda-time date object (Duration, Partial or Instant).
     *
     * @param type  The XSD type for the given value.
     * @param value The date value.
     *
     * @return A ReadablePartial, ReadableInstant, or ReadableDuration in case of success, NULL otherwise.
     *
     * @throws NotImplementedException  If the type is xsd:gDay or xsd:gMonth, which have not been implemented yet.
     * @throws IllegalArgumentException If the type cannot be parsed, or is not an XSD date type. See {@link
     *                                  #isTemporalDataType}.
     */
    public static Comparable parseTemporalValue(String type, String value) throws NotImplementedException, IllegalArgumentException {
        XSDVocabulary xsType = getXSDType(type);
        if (xsType == null) {
            throw new IllegalArgumentException("Could not determine XSD Type from " + type);
        }
        switch (xsType) {
            case DATE:
                return LocalDate.parse(value, ISODateTimeFormat.dateOptionalTimeParser());
            case DATE_TIME:
            case DATE_TIME_STAMP:
                return DateTime.parse(value);
            case DURATION:
                return Duration.parse(value);
            case G_DAY: // @todo
                throw new NotImplementedException("xsd:gDay is not supported yet.");
            case G_MONTH: // @todo
                throw new NotImplementedException("xsd:gMonth is not supported yet.");
            case G_MONTH_DAY:
                return MonthDay.parse(value);
            case G_YEAR:
                return Year.parse(value, ISODateTimeFormat.dateOptionalTimeParser());
            case G_YEAR_MONTH:
                return YearMonth.parse(value, ISODateTimeFormat.dateOptionalTimeParser());
            case TIME:
                return LocalTime.parse(value);
        }

        return null;
    }

}

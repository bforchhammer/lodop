package de.uni_potsdam.hpi.loddp.udf.util;

import org.joda.time.*;
import org.joda.time.base.BasePartial;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Locale;

/**
 * Yoda Time class for "Year-only" local dates.
 */
public final class Year
    extends BasePartial
    implements ReadablePartial, Serializable {

    /**
     * The index of the year field in the field array
     */
    public static final int YEAR = 0;
    /**
     * The singleton set of field types
     */
    private static final DateTimeFieldType[] FIELD_TYPES = new DateTimeFieldType[] {
        DateTimeFieldType.year(),
    };
    private static final long serialVersionUID = -6207396963532354898L;

    /**
     * Constructs a Year with the current year, using ISOChronology in the default zone to extract the fields. <p> The
     * constructor uses the default time zone, resulting in the local time being initialised. Once the constructor is
     * complete, all further calculations are performed without reference to a time-zone (by switching to UTC).
     *
     * @see #now()
     */
    public Year() {
        super();
    }

    /**
     * Constructs a Year with the current year, using ISOChronology in the specified zone to extract the fields. <p> The
     * constructor uses the specified time zone to obtain the current year-month. Once the constructor is complete, all
     * further calculations are performed without reference to a time-zone (by switching to UTC).
     *
     * @param zone the zone to use, null means default zone
     *
     * @see #now(DateTimeZone)
     */
    public Year(DateTimeZone zone) {
        super(ISOChronology.getInstance(zone));
    }

    /**
     * Constructs a Year with the current year, using the specified chronology and zone to extract the fields. <p> The
     * constructor uses the time zone of the chronology specified. Once the constructor is complete, all further
     * calculations are performed without reference to a time-zone (by switching to UTC).
     *
     * @param chronology the chronology, null means ISOChronology in the default zone
     *
     * @see #now(Chronology)
     */
    public Year(Chronology chronology) {
        super(chronology);
    }

    /**
     * Constructs a Year extracting the partial fields from the specified milliseconds using the ISOChronology in the
     * default zone. <p> The constructor uses the default time zone, resulting in the local time being initialised. Once
     * the constructor is complete, all further calculations are performed without reference to a time-zone (by
     * switching to UTC).
     *
     * @param instant the milliseconds from 1970-01-01T00:00:00Z
     */
    public Year(long instant) {
        super(instant);
    }

    /**
     * Constructs a Year extracting the partial fields from the specified milliseconds using the chronology provided.
     * <p> The constructor uses the time zone of the chronology specified. Once the constructor is complete, all further
     * calculations are performed without reference to a time-zone (by switching to UTC).
     *
     * @param instant    the milliseconds from 1970-01-01T00:00:00Z
     * @param chronology the chronology, null means ISOChronology in the default zone
     */
    public Year(long instant, Chronology chronology) {
        super(instant, chronology);
    }

    /**
     * Constructs a Year from an Object that represents some form of time. <p> The recognised object types are defined
     * in {@link org.joda.time.convert.ConverterManager ConverterManager} and include ReadableInstant, String, Calendar
     * and Date. The String formats are described by {@link ISODateTimeFormat#localDateParser()}. <p> The chronology
     * used will be derived from the object, defaulting to ISO.
     *
     * @param instant the date-time object, null means now
     *
     * @throws IllegalArgumentException if the instant is invalid
     */
    public Year(Object instant) {
        super(instant, null, ISODateTimeFormat.localDateParser());
    }

    /**
     * Constructs a Year from an Object that represents some form of time, using the specified chronology. <p> The
     * recognised object types are defined in {@link org.joda.time.convert.ConverterManager ConverterManager} and
     * include ReadableInstant, String, Calendar and Date. The String formats are described by {@link
     * ISODateTimeFormat#localDateParser()}. <p> The constructor uses the time zone of the chronology specified. Once
     * the constructor is complete, all further calculations are performed without reference to a time-zone (by
     * switching to UTC). The specified chronology overrides that of the object.
     *
     * @param instant    the date-time object, null means now
     * @param chronology the chronology, null means ISO default
     *
     * @throws IllegalArgumentException if the instant is invalid
     */
    public Year(Object instant, Chronology chronology) {
        super(instant, DateTimeUtils.getChronology(chronology), ISODateTimeFormat.localDateParser());
    }

    /**
     * Constructs a Year with specified year using <code>ISOChronology</code>. <p> The constructor uses the no time zone
     * initialising the fields as provided. Once the constructor is complete, all further calculations are performed
     * without reference to a time-zone (by switching to UTC).
     *
     * @param year the year
     */
    public Year(int year) {
        this(year, null);
    }

    /**
     * Constructs an instance set to the specified year using the specified chronology, whose zone is ignored. <p> If
     * the chronology is null, <code>ISOChronology</code> is used. <p> The constructor uses the time zone of the
     * chronology specified. Once the constructor is complete, all further calculations are performed without reference
     * to a time-zone (by switching to UTC).
     *
     * @param year       the year
     * @param chronology the chronology, null means ISOChronology in the default zone
     */
    public Year(int year, Chronology chronology) {
        super(new int[] {year}, chronology);
    }

    /**
     * Constructs a Year with chronology from this instance and new values.
     *
     * @param partial the partial to base this new instance on
     * @param values  the new set of values
     */
    Year(Year partial, int[] values) {
        super(partial, values);
    }

    /**
     * Constructs a Year with values from this instance and a new chronology.
     *
     * @param partial the partial to base this new instance on
     * @param chrono  the new chronology
     */
    Year(Year partial, Chronology chrono) {
        super(partial, chrono);
    }

    /**
     * Obtains a {@code Year} set to the current system millisecond time using <code>ISOChronology</code> in the default
     * time zone. The resulting object does not use the zone.
     *
     * @return the current year, not null
     */
    public static Year now() {
        return new Year();
    }

    /**
     * Obtains a {@code Year} set to the current system millisecond time using <code>ISOChronology</code> in the
     * specified time zone. The resulting object does not use the zone.
     *
     * @param zone the time zone, not null
     *
     * @return the current year, not null
     *
     * @since 2.0
     */
    public static Year now(DateTimeZone zone) {
        if (zone == null) {
            throw new NullPointerException("Zone must not be null");
        }
        return new Year(zone);
    }

    /**
     * Obtains a {@code Year} set to the current system millisecond time using the specified chronology. The resulting
     * object does not use the zone.
     *
     * @param chronology the chronology, not null
     *
     * @return the current year, not null
     *
     * @since 2.0
     */
    public static Year now(Chronology chronology) {
        if (chronology == null) {
            throw new NullPointerException("Chronology must not be null");
        }
        return new Year(chronology);
    }

    /**
     * Parses a {@code Year} from the specified string. <p> This uses {@link org.joda.time.format.ISODateTimeFormat#localDateParser()}.
     *
     * @param str the string to parse, not null
     *
     * @since 2.0
     */
    public static Year parse(String str) {
        return parse(str, ISODateTimeFormat.localDateParser());
    }

    /**
     * Parses a {@code Year} from the specified string using a formatter.
     *
     * @param str       the string to parse, not null
     * @param formatter the formatter to use, not null
     *
     * @since 2.0
     */
    public static Year parse(String str, DateTimeFormatter formatter) {
        LocalDate date = formatter.parseLocalDate(str);
        return new Year(date.getYear());
    }

    /**
     * Constructs a Year from a <code>java.util.Calendar</code> using exactly the same field values avoiding any time
     * zone effects. <p> Each field is queried from the Calendar and assigned to the Year. <p> This factory method
     * ignores the type of the calendar and always creates a Year with ISO chronology. It is expected that you will only
     * pass in instances of <code>GregorianCalendar</code> however this is not validated.
     *
     * @param calendar the Calendar to extract fields from
     *
     * @return the created Year, never null
     *
     * @throws IllegalArgumentException if the calendar is null
     * @throws IllegalArgumentException if the year is invalid for the ISO chronology
     */
    public static Year fromCalendarFields(Calendar calendar) {
        if (calendar == null) {
            throw new IllegalArgumentException("The calendar must not be null");
        }
        return new Year(calendar.get(Calendar.YEAR));
    }

    /**
     * Handle broken serialization from other tools.
     *
     * @return the resolved object, not null
     */
    private Object readResolve() {
        if (DateTimeZone.UTC.equals(getChronology().getZone()) == false) {
            return new Year(this, getChronology().withUTC());
        }
        return this;
    }

    /**
     * Gets the number of fields in this partial, which is one. The supported field is Year. Note that only these fields
     * may be queried.
     *
     * @return the field count, two
     */
    public int size() {
        return 1;
    }

    /**
     * Gets the field for a specific index in the chronology specified. <p> This method must not use any instance
     * variables.
     *
     * @param index  the index to retrieve
     * @param chrono the chronology to use
     *
     * @return the field, never null
     */
    protected DateTimeField getField(int index, Chronology chrono) {
        switch (index) {
            case YEAR:
                return chrono.year();
            default:
                throw new IndexOutOfBoundsException("Invalid index: " + index);
        }
    }

    /**
     * Gets the field type at the specified index.
     *
     * @param index the index to retrieve
     *
     * @return the field at the specified index, never null
     *
     * @throws IndexOutOfBoundsException if the index is invalid
     */
    public DateTimeFieldType getFieldType(int index) {
        return FIELD_TYPES[index];
    }

    /**
     * Gets an array of the field type of each of the fields that this partial supports. <p> The fields are returned
     * largest to smallest, Year, Month.
     *
     * @return the array of field types (cloned), largest to smallest, never null
     */
    public DateTimeFieldType[] getFieldTypes() {
        return (DateTimeFieldType[]) FIELD_TYPES.clone();
    }

    /**
     * Returns a copy of this year with the specified chronology. This instance is immutable and unaffected by this
     * method call. <p> This method retains the values of the fields, thus the result will typically refer to a
     * different instant. <p> The time zone of the specified chronology is ignored, as YearMonth operates without a time
     * zone.
     *
     * @param newChronology the new chronology, null means ISO
     *
     * @return a copy of this year with a different chronology, never null
     *
     * @throws IllegalArgumentException if the values are invalid for the new chronology
     */
    public Year withChronologyRetainFields(Chronology newChronology) {
        newChronology = DateTimeUtils.getChronology(newChronology);
        newChronology = newChronology.withUTC();
        if (newChronology == getChronology()) {
            return this;
        } else {
            Year newYearMonth = new Year(this, newChronology);
            newChronology.validate(newYearMonth, getValues());
            return newYearMonth;
        }
    }

    /**
     * Returns a copy of this year with the specified field set to a new value.
     *
     * @param fieldType the field type to set, not null
     * @param value     the value to set
     *
     * @return a copy of this instance with the field set, never null
     *
     * @throws IllegalArgumentException if the value is null or invalid
     */
    public Year withField(DateTimeFieldType fieldType, int value) {
        int index = indexOfSupported(fieldType);
        if (value == getValue(index)) {
            return this;
        }
        int[] newValues = getValues();
        newValues = getField(index).set(this, index, newValues, value);
        return new Year(this, newValues);
    }

    /**
     * Converts this object to a LocalDate with the same year and chronology.
     *
     * @param monthOfYear the month of year to use.
     * @param dayOfMonth  the day of month to use, valid for chronology, such as 1-31 for ISO
     *
     * @return a LocalDate with the same year-month and chronology, never null
     */
    public LocalDate toLocalDate(int monthOfYear, int dayOfMonth) {
        return new LocalDate(getYear(), monthOfYear, dayOfMonth, getChronology());
    }

    /**
     * Get the year field value.
     *
     * @return the year
     */
    public int getYear() {
        return getValue(YEAR);
    }

    /**
     * Returns a copy of this year-month with the year field updated. <p> YearMonth is immutable, so there are no set
     * methods. Instead, this method returns a new instance with the value of year changed.
     *
     * @param year the year to set
     *
     * @return a copy of this object with the field set, never null
     *
     * @throws IllegalArgumentException if the value is invalid
     */
    public Year withYear(int year) {
        int[] newValues = getValues();
        newValues = getChronology().year().set(this, YEAR, newValues, year);
        return new Year(this, newValues);
    }

    /**
     * Output the year-month in ISO8601 format (yyyy).
     *
     * @return ISO8601 time formatted string.
     */
    public String toString() {
        return ISODateTimeFormat.year().print(this);
    }

    /**
     * Output the year-month using the specified format pattern.
     *
     * @param pattern the pattern specification, null means use <code>toString</code>
     *
     * @see org.joda.time.format.DateTimeFormat
     */
    public String toString(String pattern) {
        if (pattern == null) {
            return toString();
        }
        return DateTimeFormat.forPattern(pattern).print(this);
    }

    /**
     * Output the year using the specified format pattern.
     *
     * @param pattern the pattern specification, null means use <code>toString</code>
     * @param locale  Locale to use, null means default
     *
     * @see org.joda.time.format.DateTimeFormat
     */
    public String toString(String pattern, Locale locale) throws IllegalArgumentException {
        if (pattern == null) {
            return toString();
        }
        return DateTimeFormat.forPattern(pattern).withLocale(locale).print(this);
    }
}

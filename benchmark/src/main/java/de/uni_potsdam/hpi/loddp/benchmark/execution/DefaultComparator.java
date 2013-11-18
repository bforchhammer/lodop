package de.uni_potsdam.hpi.loddp.benchmark.execution;

import java.util.Comparator;

/**
 * The Default Comparator for classes implementing Comparable.
 *
 * @param <E> the type of the comparable objects.
 *
 * @author Michael Belivanakis (michael.gr)
 */
public final class DefaultComparator<E extends Comparable<E>> implements Comparator<E> {
    @SuppressWarnings("rawtypes")
    private static final DefaultComparator<?> INSTANCE = new DefaultComparator();

    private DefaultComparator() {
    }

    /**
     * Get an instance of DefaultComparator for any type of Comparable.
     *
     * @param <T> the type of Comparable of interest.
     *
     * @return an instance of DefaultComparator for comparing instances of the requested type.
     */
    public static <T extends Comparable<T>> Comparator<T> getInstance() {
        @SuppressWarnings("unchecked")
        Comparator<T> result = (Comparator<T>) INSTANCE;
        return result;
    }

    @Override
    public int compare(E o1, E o2) {
        if (o1 == o2)
            return 0;
        if (o1 == null)
            return 1;
        if (o2 == null)
            return -1;
        return o1.compareTo(o2);
    }
}
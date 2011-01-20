package org.scale7.cassandra.pelops;

import com.eaio.uuid.UUIDGen;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

public class UuidHelper {
    /*
      Magic number obtained from #cassandra's thobbs, who
      claims to have stolen it from a Python library.
    */
    static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	/**
	 * Generate a new time UUID insatnce.
	 * @return							A new time UUID object
	 */
	public static java.util.UUID newTimeUuid() {
		return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
	}

	/**
	 * Generate a new time UUID instance from the given timestamp.  As mentioned on the {@link com.eaio.uuid.UUIDGen} javadoc
     * even identical values of <code>millis</code> will produce different UUIDs.
     * <p>Note: don't confuse this with {@link #timeUuidForDate(long)}!
	 * @param millis the timestamp to use
     * @return A new time UUID object
	 */
	public static java.util.UUID newTimeUuid(long millis) {
		return java.util.UUID.fromString(
                new com.eaio.uuid.UUID(UUIDGen.createTime(millis), UUIDGen.getClockSeqAndNode()).toString()
        );
	}

	/**
	 * Generate a new time UUID instance with a UTC timestamp.  Using this method to generate UUIDs ensures that
     * you can perform time based range scans on (super) column families that use the TimeUUID comparator.  It generates
     * a UUID with a UTC timestamp that means your machine can change adjust timezones (e.g. daylight savings) and still
     * fetch the current data.
     * <p><b>NOTE</b>: This requires the optional Joda-Time dependency.
	 * @return							A new time UUID object
	 */
	public static java.util.UUID newTimeUuidUTC() {
		return java.util.UUID.fromString(
                new com.eaio.uuid.UUID(
                        UUIDGen.createTime(
                                DateTimeZone.UTC.convertLocalToUTC(System.currentTimeMillis(), true)
                        ),
                        UUIDGen.getClockSeqAndNode()
                ).toString()
        );
	}

    /**
     * @see #timeUuidForDate(long)
     */
    public static java.util.UUID timeUuidForDate(Date d) {
        return timeUuidForDate(d.getTime());
    }

    /**
     * @see #timeUuidForDate(long)
     */
    public static java.util.UUID timeUuidForDate(DateTime d) {
        return timeUuidForDate(d.getMillis());
    }

    /**
     * Creates a TimeUUID using the provided DateTime instance.  Additionally, creates a new instance of the provided
     * date in UTC if it's not already.
     * <p>This is most useful when used in conjunction with the {@link #newTimeUuidUTC()} method to create dates.
     * @see #timeUuidForDate(long)
     */
    public static java.util.UUID timeUuidForDateUTC(DateTime d) {
        return timeUuidForDate(d.toDateTime(DateTimeZone.UTC));
    }

    /**
     * <p>This method is useful to create a non-unique TimeUUID instance from some time other than the present.
     * For example, to use as the lower bound in a SlicePredicate to retrieve all columns whose TimeUUID comes
     * after time X.
     *
     * <p><b>WARNING:</b> Never assume such a UUID is unique, use it only as a marker for a specific time.
     * <p>Note: This method and it's doco is taken (almost verbatim) from the Cassandra WIKI
     * (http://wiki.apache.org/cassandra/FAQ#working_with_timeuuid_in_java).
     * @param millis Gets the milliseconds of the datetime instant from the Java epoch of 1970-01-01T00:00:00Z
     * @return return
     */
    public static java.util.UUID timeUuidForDate(long millis) {
        long time = millis * 10000 + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
        long timeLow = time &       0xffffffffL;
        long timeMid = time &   0xffff00000000L;
        long timeHi = time & 0xfff000000000000L;
        long upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12) | (timeHi >> 48) ;
        return new java.util.UUID(upperLong, 0xC000000000000000L);
    }

    /**
     * <p>Extracts the millis from the Java epoch of 1970-01-01T00:00:00Z.</p>
     * @param uuid the time uuid
     * @return the millis since Java epoch
     */
    public static long millisFromTimeUuid(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }
}

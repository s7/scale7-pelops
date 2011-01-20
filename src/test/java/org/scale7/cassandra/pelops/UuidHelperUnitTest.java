package org.scale7.cassandra.pelops;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.scale7.cassandra.pelops.UuidHelper.*;

public class UuidHelperUnitTest {
    @Test
    public void testTimeUuidForDate() {
        long millisSource = System.currentTimeMillis();

        UUID uuid = timeUuidForDate(millisSource);

        long millisFromUuid = millisFromTimeUuid(uuid);

        Assert.assertEquals("Timestamp was not equal to source", millisSource, millisFromUuid);
    }

    @Test
    public void testNewTimeUuidUTC() {
        DateTime utcStart = new DateTime(DateTimeZone.UTC);
        UUID uuid = newTimeUuidUTC();
        DateTime utcEnd = new DateTime(DateTimeZone.UTC);

        long millisFromUuid = millisFromTimeUuid(uuid);

        Assert.assertTrue("The new UTC UUID was does not contain a UTC based millis from epoch.",
                utcStart.getMillis() <= millisFromUuid && utcEnd.getMillis() >= millisFromUuid);
    }

    @Test
    public void testNewTimeUuidWithMillis() {
        final long millis = System.currentTimeMillis();
        UUID uuid = newTimeUuid(millis);

        long millisFromUuid = millisFromTimeUuid(uuid);

        Assert.assertEquals(millis, millisFromUuid);
    }
}

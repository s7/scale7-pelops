package org.scale7.cassandra.pelops;

import static org.junit.Assert.assertEquals;
import static org.scale7.cassandra.pelops.UuidHelper.millisFromTimeUuid;
import static org.scale7.cassandra.pelops.UuidHelper.nonUniqueTimeUuidForDate;

import java.util.UUID;

import org.junit.Test;

public class UuidHelperUnitTest {
    @Test
    public void testTimeUuidForDate() {
        long millisSource = System.currentTimeMillis();

        UUID uuid = nonUniqueTimeUuidForDate(millisSource);

        long millisFromUuid = millisFromTimeUuid(uuid);

        assertEquals("Timestamp was not equal to source", millisSource, millisFromUuid);
    }
}

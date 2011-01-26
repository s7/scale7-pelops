package org.scale7.cassandra.pelops;

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;
import static org.scale7.cassandra.pelops.UuidHelper.millisFromTimeUuid;
import static org.scale7.cassandra.pelops.UuidHelper.nonUniqueTimeUuidForDate;

public class UuidHelperUnitTest {
    @Test
    public void testTimeUuidForDate() {
        long millisSource = System.currentTimeMillis();

        UUID uuid = nonUniqueTimeUuidForDate(millisSource);

        long millisFromUuid = millisFromTimeUuid(uuid);

        assertEquals("Timestamp was not equal to source", millisSource, millisFromUuid);
    }
}

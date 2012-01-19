package org.scale7.cassandra.pelops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Test;

/**
 * Unit test of the {@link BlockingCallback} class.
 * 
 * @author Andrew Swan
 */
public class BlockingCallbackTest {

    @Test
    public void testGetResultWhenOperationSucceeds() throws Exception {
        // Set up
        final BlockingCallback<Long> callback = new BlockingCallback<Long>();
        final long result = 29;
        callback.onComplete(result);

        // Invoke and check
        assertEquals(result, callback.getResult().longValue());
    }

    @Test
    public void testGetResultWhenOperationFails() {
        // Set up
        final BlockingCallback<Long> callback = new BlockingCallback<Long>();
        final Exception mockException = mock(Exception.class);
        callback.onError(mockException);

        // Invoke and check
        try {
            callback.getResult();
            fail("Expected an exception");
        }
        catch (Exception e) {
            assertEquals(mockException, e);
        }
    }
}

package org.scale7.cassandra.pelops.exceptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.SocketException;

import org.junit.Test;

/**
 * Tests the {@link IExceptionTranslator.ExceptionTranslator} class.
 */
public class ExceptionTranslatorUnitTest {
    private IExceptionTranslator.ExceptionTranslator translator = new IExceptionTranslator.ExceptionTranslator();

    @Test
    public void testTranslateNotFoundException() {
        PelopsException pelopsException = translator.translate(new org.apache.cassandra.thrift.NotFoundException());
        assertEquals("Translation failed", NotFoundException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateInvalidRequestException() {
        PelopsException pelopsException = translator.translate(new org.apache.cassandra.thrift.InvalidRequestException());
        assertEquals("Translation failed", InvalidRequestException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateApplicationException() {
        PelopsException pelopsException = translator.translate(new org.apache.thrift.TApplicationException());
        assertEquals("Translation failed", ApplicationException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateAuthenticationException() {
        PelopsException pelopsException = translator.translate(new org.apache.cassandra.thrift.AuthenticationException());
        assertEquals("Translation failed", AuthenticationException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateAuthorizationException() {
        PelopsException pelopsException = translator.translate(new org.apache.cassandra.thrift.AuthorizationException());
        assertEquals("Translation failed", AuthorizationException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateTimedOutException() {
        PelopsException pelopsException = translator.translate(new org.apache.cassandra.thrift.TimedOutException());
        assertEquals("Translation failed", TimedOutException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateTransportException() {
        PelopsException pelopsException = translator.translate(new org.apache.thrift.transport.TTransportException());
        assertEquals("Translation failed", TransportException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateProtocolException() {
        PelopsException pelopsException = translator.translate(new org.apache.thrift.protocol.TProtocolException());
        assertEquals("Translation failed", ProtocolException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslateUnavailableException() {
        PelopsException pelopsException = translator.translate(new org.apache.cassandra.thrift.UnavailableException());
        assertEquals("Translation failed", UnavailableException.class, pelopsException.getClass());
    }

    @Test
    public void testTranslatePelopsException() {
        PelopsException exception = new PelopsException();
        PelopsException pelopsException = translator.translate(exception);
        assertTrue("Translation failed", exception == pelopsException);
    }

    @Test
    public void testTranslateOtherException() {
        PelopsException pelopsException = translator.translate(new SocketException());
        assertEquals("Translation failed", PelopsException.class, pelopsException.getClass());
    }
}

package org.scale7.cassandra.pelops.exceptions;

/**
 * Interface used to define how exception are translated.
 */
public interface IExceptionTranslator {
    /**
     * Translate the provided exception into a pelops exception.
     * @param e the exception
     * @return the pelops exception
     */
    PelopsException translate(Exception e);

    /**
     * Default implementation.
     */
    public static class ExceptionTranslator implements IExceptionTranslator {
        @Override
        public PelopsException translate(Exception e) {
            if (e instanceof org.apache.cassandra.thrift.NotFoundException)
                return new NotFoundException(e);
            else if (e instanceof org.apache.cassandra.thrift.InvalidRequestException)
                return new InvalidRequestException(e);
            else if (e instanceof org.apache.thrift.TApplicationException)
                return new ApplicationException(e);
            else if (e instanceof org.apache.cassandra.thrift.AuthenticationException)
                return new AuthenticationException(e);
            else if (e instanceof org.apache.cassandra.thrift.AuthorizationException)
                return new AuthorizationException(e);
            else if (e instanceof org.apache.cassandra.thrift.TimedOutException)
                return new TimedOutException(e);
            else if (e instanceof org.apache.thrift.transport.TTransportException)
                return new TransportException(e);
            else if (e instanceof org.apache.thrift.protocol.TProtocolException)
                return new ProtocolException(e);
            else if (e instanceof org.apache.cassandra.thrift.UnavailableException)
                return new UnavailableException(e);
            else if (e instanceof PelopsException)
                return (PelopsException) e; // don't re-wrap
            else
                return new PelopsException(e);
        }
    }
}

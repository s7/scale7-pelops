/*
 * The MIT License
 *
 * Copyright (c) 2011 Dominic Williams, Daniel Washusen and contributors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
                return new InvalidRequestException((org.apache.cassandra.thrift.InvalidRequestException) e);
            else if (e instanceof org.apache.thrift.TApplicationException)
                return new ApplicationException(e);
            else if (e instanceof org.apache.cassandra.thrift.AuthenticationException)
                return new AuthenticationException((org.apache.cassandra.thrift.AuthenticationException) e);
            else if (e instanceof org.apache.cassandra.thrift.AuthorizationException)
                return new AuthorizationException((org.apache.cassandra.thrift.AuthorizationException) e);
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

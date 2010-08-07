package org.wyki.cassandra.pelops;

import org.wyki.cassandra.pelops.IThriftPool.Connection;

public interface IOperation<ReturnType> {
	ReturnType execute(Connection conn) throws Exception;
}

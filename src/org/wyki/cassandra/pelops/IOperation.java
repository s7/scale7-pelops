package org.wyki.cassandra.pelops;

import org.wyki.cassandra.pelops.ThriftPool.Connection;

public interface IOperation {
	Object execute(Connection conn) throws Exception;
}

package org.wyki.cassandra.pelops;

public class KeyspaceOperand extends Operand {

	protected final String keyspace;
	
	protected KeyspaceOperand(ThriftPool thrift, String keyspace) {
		super(thrift);
		this.keyspace = keyspace;
	}

}
